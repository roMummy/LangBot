"""WechatReal08 personal WeChat HTTP API adapter.

The backend (wechatReal08) runs as a Docker service exposing a beego HTTP API
(swagger: wechatReal08/swagger/swagger.json). Incoming messages are pushed by
the backend to the configured ``syncmessagebusinessuri`` (see wechatReal08
conf/app.conf, ``msgpush=true`` required), which must point at this adapter's
callback endpoint::

    POST /msg/SyncMessage/{wxid}   # push body: {"Code":0,"Data":{"AddMsgs":[...]}}

The adapter performs QR-code login, starts the backend heartbeat/push loop via
``/Login/AutoHeartBeat``, and sends messages through ``/Msg/*`` endpoints.
"""

import asyncio
import base64
import copy
import hashlib
import json
import os
import random
import re
import string
import time
import traceback
import typing
import urllib.request
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape

import aiohttp
import quart

from langbot.pkg.utils import httpclient

import langbot_plugin.api.definition.abstract.platform.adapter as abstract_platform_adapter
import langbot_plugin.api.entities.builtin.platform.message as platform_message
import langbot_plugin.api.entities.builtin.platform.events as platform_events
import langbot_plugin.api.entities.builtin.platform.entities as platform_entities

DEFAULT_CALLBACK_PORT = 8088  # matches the default syncmessagebusinessuri port
QR_POLL_INTERVAL = 5
QR_POLL_TIMEOUT = 300
# Persisted login state (wxid/device_id); /app/data is a mounted volume.
STATE_FILE_PATH = '/app/data/wechat08_state.json'
# One-shot marker written after clear_cache runs once, so a config left enabled
# does not wipe the login on every container restart.
CLEAR_CACHE_FLAG_PATH = '/app/data/wechat08_clear_cache.flag'


class Wechat08MessageConverter(abstract_platform_adapter.AbstractMessageConverter):
    def __init__(self, config: dict, logger):
        self.config = config
        self.logger = logger

    @staticmethod
    async def yiri2target(message_chain: platform_message.MessageChain) -> list[dict]:
        """Convert a LangBot MessageChain into wechat08 send payloads."""
        content_list = []
        for component in message_chain:
            if isinstance(component, platform_message.AtAll):
                content_list.append({'type': 'at', 'target': 'all'})
            elif isinstance(component, platform_message.At):
                content_list.append({'type': 'at', 'target': component.target})
            elif isinstance(component, platform_message.Plain):
                content_list.append({'type': 'text', 'content': component.text})
            elif isinstance(component, platform_message.Image):
                if component.url:
                    session = httpclient.get_session()
                    async with session.get(component.url) as response:
                        if response.status != 200:
                            raise Exception('failed to download image url')
                        file_bytes = await response.read()
                    base64_str = (await asyncio.to_thread(base64.b64encode, file_bytes)).decode('utf-8')
                    content_list.append({'type': 'image', 'base64': base64_str})
                elif component.base64:
                    content_list.append({'type': 'image', 'base64': component.base64})
                elif component.path:
                    # Read a local image file (e.g. a plugin-generated image on a shared volume)
                    def _read_local_image(file_path: str) -> bytes:
                        with open(file_path, 'rb') as f:
                            return f.read()

                    file_bytes = await asyncio.to_thread(_read_local_image, str(component.path))
                    base64_str = (await asyncio.to_thread(base64.b64encode, file_bytes)).decode('utf-8')
                    content_list.append({'type': 'image', 'base64': base64_str})
            elif isinstance(component, platform_message.File):
                if component.path:
                    # Read a local file (e.g. a plugin-generated PDF on a shared volume)
                    def _read_local_file(file_path: str) -> bytes:
                        with open(file_path, 'rb') as f:
                            return f.read()

                    file_bytes = await asyncio.to_thread(_read_local_file, str(component.path))
                elif component.base64:
                    base64_data = component.base64
                    if ',' in base64_data and base64_data.split(',', 1)[0].startswith('data:'):
                        base64_data = base64_data.split(',', 1)[-1]
                    file_bytes = await asyncio.to_thread(base64.b64decode, base64_data)
                else:
                    raise Exception('file component has neither path nor base64')
                base64_str = (await asyncio.to_thread(base64.b64encode, file_bytes)).decode('utf-8')
                content_list.append({'type': 'file', 'base64': base64_str, 'name': component.name or 'file'})
            elif isinstance(component, platform_message.Voice):
                content_list.append(
                    {
                        'type': 'voice',
                        'base64': getattr(component, 'base64', None) or component.url,
                        'duration': component.length,
                        'forma': 4,  # SILK
                    }
                )
            elif isinstance(component, platform_message.WeChatEmoji):
                content_list.append(
                    {'type': 'WeChatEmoji', 'emoji_md5': component.emoji_md5, 'emoji_size': component.emoji_size}
                )
            elif isinstance(component, platform_message.WeChatAppMsg):
                content_list.append({'type': 'WeChatAppMsg', 'app_msg': component.app_msg})
            elif isinstance(component, platform_message.WeChatForwardQuote):
                content_list.append({'type': 'WeChatAppMsg', 'app_msg': component.app_msg})
            elif isinstance(component, platform_message.Forward):
                for node in component.node_list:
                    if node.message_chain:
                        content_list.extend(await Wechat08MessageConverter.yiri2target(node.message_chain))
        return content_list

    async def target2yiri(self, message: dict, bot_account_id: str) -> platform_message.MessageChain:
        """Convert a pushed AddMsg dict into a LangBot MessageChain."""
        message_list = []
        try:
            content = message.get('Content', {}).get('string', '') or ''
        except AttributeError:
            content = ''
        content_no_prefix = content
        msg_type = message.get('MsgType', 0)

        is_group_message = self._is_group_message(message)
        if is_group_message:
            # generate At components from the group message
            if self._ats_bot(message, bot_account_id):
                message_list.append(platform_message.At(target=bot_account_id))
            for target_id in self._extract_at_targets(message):
                if target_id and target_id != bot_account_id:
                    message_list.append(platform_message.At(target=target_id))
            content_no_prefix, _ = self._extract_content_and_sender(content)

        handler_map = {
            1: self._handler_text,
            3: self._handler_image,
            34: self._handler_voice,
            49: self._handler_compound,
        }
        handler = handler_map.get(msg_type, self._handler_default)
        handler_result = await handler(message, content_no_prefix)
        if handler_result:
            message_list.extend(handler_result)

        return platform_message.MessageChain(message_list)

    async def _handler_text(self, message: dict, content_no_prefix: str) -> list:
        """Handle text message (MsgType=1)."""
        if self._is_group_message(message):
            # strip @wxid markers and the sender prefix
            content_no_prefix = re.sub(r'@[a-zA-Z0-9_\-]{5,32}', '', content_no_prefix)
        return [platform_message.Plain(text=content_no_prefix)]

    async def _handler_image(self, message: dict, content_no_prefix: str) -> list:
        """Handle image message (MsgType=3): download via /Tools/CdnDownloadImage."""
        try:
            if not content_no_prefix:
                return [platform_message.Unknown(text='[图片内容为空]')]
            root = ET.fromstring(content_no_prefix)
            img_tag = root.find('img')
            aeskey = img_tag.get('aeskey', '') if img_tag is not None else ''
            cdnthumburl = img_tag.get('cdnthumburl', '') if img_tag is not None else ''

            base64_str = await self._download_cdn_image(aeskey, cdnthumburl)
            if base64_str:
                return [
                    platform_message.Image(base64=f'data:image/jpg;base64,{base64_str}'),
                    platform_message.WeChatForwardImage(xml_data=content_no_prefix),
                ]
            return [platform_message.Unknown(text='[图片下载失败]')]
        except Exception as e:
            await self.logger.error(f'wechat08 image parse failed: {e}')
            return [platform_message.Unknown(text='[图片处理失败]')]

    async def _download_cdn_image(self, aeskey: str, cdnthumburl: str) -> str:
        """Download a CDN image and return raw base64, or '' on failure."""
        if not aeskey or not cdnthumburl:
            return ''
        # FileNo is the file name part of the cdn url
        file_no = cdnthumburl.split('?')[0].rstrip('/').split('/')[-1]
        result = await self._api_post(
            '/Tools/CdnDownloadImage',
            body={'Wxid': self.config['wxid'], 'FileAesKey': aeskey, 'FileNo': file_no},
        )
        data = result.get('Data') or {}
        return data.get('Image') or ''

    async def _handler_voice(self, message: dict, content_no_prefix: str) -> list:
        """Handle voice message (MsgType=34): download via /Tools/DownloadVoice."""
        try:
            audio_base64 = ''
            # download the voice from the server first
            try:
                root = ET.fromstring(content_no_prefix)
                voicemsg = root.find('voicemsg')
                if voicemsg is not None:
                    bufid = voicemsg.get('bufid', '')
                    length = voicemsg.get('voicelength', '0')
                    if bufid:
                        result = await self._api_post(
                            '/Tools/DownloadVoice',
                            body={
                                'Wxid': self.config['wxid'],
                                'FromUserName': (message.get('FromUserName') or {}).get('string', ''),
                                'MsgId': message.get('MsgId') or 0,
                                'Bufid': bufid,
                                'Length': int(length or 0),
                            },
                        )
                        voice_data = (result.get('Data') or {}).get('data') or {}
                        audio_base64 = voice_data.get('buffer') or ''
            except Exception as e:
                await self.logger.debug(f'语音下载失败，回退 ImgBuf: {e}')
            if not audio_base64:
                audio_base64 = (message.get('ImgBuf') or {}).get('buffer') or ''
            if not audio_base64:
                return [platform_message.Unknown(text='[语音内容为空]')]
            return [platform_message.Voice(base64=f'data:audio/silk;base64,{audio_base64}')]
        except Exception as e:
            await self.logger.error(f'wechat08 voice parse failed: {e}')
            return [platform_message.Unknown(text='[语音处理失败]')]

    async def _handler_compound(self, message: dict, content_no_prefix: str) -> list:
        """Handle compound message (MsgType=49), dispatch by appmsg type."""
        try:
            xml_data = ET.fromstring(content_no_prefix)
            appmsg_data = xml_data.find('.//appmsg')
            if appmsg_data is None:
                return [platform_message.Unknown(text=content_no_prefix)]
            data_type = appmsg_data.findtext('.//type', '')
            if data_type == '57':  # quote message
                return self._handler_compound_quote(xml_data, appmsg_data)
            if data_type == '5':  # link message
                return [
                    platform_message.WeChatLink(
                        link_title=appmsg_data.findtext('title', ''),
                        link_desc=appmsg_data.findtext('des', ''),
                        link_url=appmsg_data.findtext('url', ''),
                        link_thumb_url=appmsg_data.findtext('thumburl', ''),
                    ),
                    platform_message.WeChatForwardLink(xml_data=content_no_prefix),
                ]
            if data_type == '6':  # file message
                return [platform_message.WeChatForwardFile(xml_data=content_no_prefix)]
            if data_type == '74':  # mini-program image (no useful content), drop it
                return None
            if data_type in ('33', '36'):  # mini program
                return [platform_message.WeChatForwardMiniPrograms(xml_data=content_no_prefix)]
            unsupported_texts = {
                '2000': '[转账消息]',
                '2001': '[红包消息]',
                '51': '[视频号消息]',
            }
            if data_type in unsupported_texts:
                return [platform_message.Unknown(text=unsupported_texts[data_type])]
            return [platform_message.Unknown(text=f'[未支持的复合消息类型 {data_type}]')]
        except Exception as e:
            await self.logger.error(f'wechat08 compound parse failed: {e}')
            return [platform_message.Unknown(text=content_no_prefix)]

    def _handler_compound_quote(self, xml_data: ET.Element, appmsg_data: ET.Element) -> list:
        """Handle quote message (appmsg type=57)."""
        message_list = []
        quote_data = appmsg_data.find('.//refermsg').findtext('.//content', '') if appmsg_data.find('.//refermsg') is not None else ''
        user_data = appmsg_data.findtext('.//title', '') or ''
        sender_id = xml_data.findtext('.//fromusername', '')
        message_list.append(
            platform_message.WeChatForwardQuote(app_msg=ET.tostring(appmsg_data, encoding='unicode'))
        )
        if quote_data:
            message_list.append(platform_message.Quote(sender_id=sender_id, origin=[platform_message.Plain(text=quote_data)]))
        if user_data:
            message_list.append(platform_message.Plain(text=re.sub(r'@[a-zA-Z0-9_\-]{5,32}', '', user_data)))
        return message_list

    async def _handler_default(self, message: dict, content_no_prefix: str) -> list:
        """Handle unknown message types."""
        return [platform_message.Unknown(text=f'[未知消息类型 msg_type:{message.get("MsgType", 0)}]')]

    def _is_group_message(self, message: dict) -> bool:
        from_user_name = (message.get('FromUserName') or {}).get('string', '')
        return from_user_name.endswith('@chatroom')

    def _extract_content_and_sender(self, raw_content: str):
        """Strip the leading '<sender_wxid>:' prefix used in group messages."""
        try:
            regex = re.compile(r'^[a-zA-Z0-9_\-]{5,20}:')
            line_split = raw_content.split('\n')
            if len(line_split) > 0 and regex.match(line_split[0]):
                return '\n'.join(line_split[1:]), line_split[0].strip(':')
        except Exception:
            pass
        return raw_content, None

    def _ats_bot(self, message: dict, bot_account_id: str) -> bool:
        """Check whether the bot was mentioned in a group message."""
        ats_bot = False
        try:
            push_content = message.get('PushContent', '') or ''
            ats_bot = ats_bot or ('在群聊中@了你' in push_content)
            msg_source = message.get('MsgSource', '') or ''
            if msg_source:
                msg_source_data = ET.fromstring(msg_source)
                at_user_list = msg_source_data.findtext('atuserlist') or ''
                ats_bot = ats_bot or (bot_account_id in at_user_list)
            if message.get('MsgType', 0) == 49:
                raw_content = (message.get('Content') or {}).get('string', '')
                xml_data = ET.fromstring(raw_content)
                appmsg_data = xml_data.find('.//appmsg')
                if appmsg_data is not None and appmsg_data.find('.//refermsg') is not None:
                    quote_id = appmsg_data.find('.//refermsg').findtext('.//chatusr', '')
                    ats_bot = ats_bot or (quote_id == bot_account_id)
        except Exception:
            pass
        return ats_bot

    def _extract_at_targets(self, message: dict) -> list[str]:
        """Extract the wxids mentioned in a group message from msg_source."""
        at_targets = []
        try:
            msg_source = message.get('MsgSource', '') or ''
            if msg_source:
                msg_source_data = ET.fromstring(msg_source)
                at_user_list = msg_source_data.findtext('atuserlist') or ''
                if at_user_list:
                    at_targets = [user_id.strip() for user_id in at_user_list.split(',') if user_id.strip()]
        except Exception:
            pass
        return at_targets


class Wechat08EventConverter(abstract_platform_adapter.AbstractEventConverter):
    def __init__(self, config: dict, logger):
        self.config = config
        self.logger = logger
        self.message_converter = Wechat08MessageConverter(config, logger)

    async def target2yiri(self, event: dict, bot_account_id: str) -> platform_events.MessageEvent | None:
        """Convert a pushed AddMsg dict into a LangBot MessageEvent."""
        # ignore messages sent by the bot itself
        from_user_name = (event.get('FromUserName') or {}).get('string', '')
        if not from_user_name:
            return None
        if from_user_name == self.config.get('wxid'):
            return None
        # ignore official accounts and system messages
        if from_user_name.startswith('gh_') or from_user_name in ('weixin', 'newsapp'):
            return None

        message_chain = await self.message_converter.target2yiri(copy.deepcopy(event), bot_account_id)
        if not message_chain:
            return None

        if '@chatroom' in from_user_name:
            sender_wxid = (event.get('Content') or {}).get('string', '').split(':')[0]
            return platform_events.GroupMessage(
                sender=platform_entities.GroupMember(
                    id=sender_wxid,
                    member_name=from_user_name,
                    permission=platform_entities.Permission.Member,
                    group=platform_entities.Group(
                        id=from_user_name,
                        name=from_user_name,
                        permission=platform_entities.Permission.Member,
                    ),
                    special_title='',
                ),
                message_chain=message_chain,
                time=event.get('CreateTime') or 0,
                source_platform_object=event,
            )
        return platform_events.FriendMessage(
            sender=platform_entities.Friend(
                id=from_user_name,
                nickname=from_user_name,
                remark='',
            ),
            message_chain=message_chain,
            time=event.get('CreateTime') or 0,
            source_platform_object=event,
        )


class Wechat08Adapter(abstract_platform_adapter.AbstractMessagePlatformAdapter):
    name: str = 'wechat08'

    # Fields must be declared on the class: the SDK base class is a pydantic
    # model, undeclared attributes cannot be assigned on instances.
    quart_app: quart.Quart
    message_converter: Wechat08MessageConverter
    event_converter: Wechat08EventConverter
    listeners: typing.Dict[
        typing.Type[platform_events.Event],
        typing.Callable[[platform_events.Event, abstract_platform_adapter.AbstractMessagePlatformAdapter], None],
    ] = {}

    def __init__(self, config: dict, logger):
        quart_app = quart.Quart(__name__)
        message_converter = Wechat08MessageConverter(config, logger)
        event_converter = Wechat08EventConverter(config, logger)
        super().__init__(
            config=config,
            logger=logger,
            quart_app=quart_app,
            message_converter=message_converter,
            event_converter=event_converter,
            listeners={},
            bot_account_id='',
        )

        @self.quart_app.route('/msg/SyncMessage/<wxid>', methods=['POST'])
        async def sync_message_callback(wxid: str):
            try:
                payload = await quart.request.get_json(silent=True)
            except Exception:
                payload = None
            if not payload or not isinstance(payload, dict):
                # heartbeat ping with non-JSON body, ignore
                return 'ok'

            add_msgs = (payload.get('Data') or {}).get('AddMsgs') or []
            await self.logger.debug(f'收到消息推送 wxid={wxid}, AddMsgs={len(add_msgs)}')
            for msg in add_msgs:
                # TEMP DEBUG: dump raw sync entries to diagnose file message relay
                try:
                    _f = ((msg.get('FromUserName') or {}).get('string') or '')
                    _t = ((msg.get('ToUserName') or {}).get('string') or '')
                    _c = ((msg.get('Content') or {}).get('string') or '')
                    await self.logger.info(
                        f'SYNCRAW from={_f} to={_t} MsgType={msg.get("MsgType")} Status={msg.get("Status")} '
                        f'NewMsgId={msg.get("NewMsgId")} content_head={_c[:220]!r}'
                    )
                except Exception as e:
                    await self.logger.warning(f'SYNCRAW dump failed: {e}')
                try:
                    event = await self.event_converter.target2yiri(msg, self.bot_account_id)
                except Exception:
                    await self.logger.error(f'Error in wechat08 callback: {traceback.format_exc()}')
                    continue
                if event is not None and event.__class__ in self.listeners:
                    await self.listeners[event.__class__](event, self)
            return 'ok'

    async def _api_post(
        self,
        path: str,
        query: dict | None = None,
        body: dict | None = None,
        form: dict | None = None,
    ) -> dict:
        base_url = self.config['base_url'].rstrip('/')
        session = httpclient.get_session()
        debug_started_at = time.monotonic() if path == '/Login/AutoHeartBeat' else None

        async def report_debug(hypothesis_id: str, message: str, data: dict):
            if path != '/Login/AutoHeartBeat':
                return
            # #region debug-point heartbeat:report
            try:
                debug_url = 'http://host.docker.internal:7777/event'
                debug_session = 'heartbeat-timeout'
                with open('.dbg/heartbeat-timeout.env', encoding='utf-8') as debug_file:
                    debug_env = debug_file.read().splitlines()
                    debug_url = next((line.split('=', 1)[1] for line in debug_env if line.startswith('DEBUG_SERVER_URL=')), debug_url)
                    debug_session = next((line.split('=', 1)[1] for line in debug_env if line.startswith('DEBUG_SESSION_ID=')), debug_session)
                debug_payload = {
                    'sessionId': debug_session,
                    'runId': 'pre-fix',
                    'hypothesisId': hypothesis_id,
                    'location': 'wechat08.py:_api_post',
                    'msg': f'[DEBUG] {message}',
                    'data': data,
                }
                await asyncio.to_thread(
                    lambda: urllib.request.urlopen(
                        urllib.request.Request(
                            debug_url,
                            data=json.dumps(debug_payload).encode(),
                            headers={'Content-Type': 'application/json'},
                        ),
                        timeout=1,
                    ).read()
                )
            except Exception:
                pass
            # #endregion

        await report_debug(
            'A',
            'AutoHeartBeat request started',
            {'path': path, 'query': query, 'body': body, 'form': form, 'base_url': base_url},
        )
        timeout = aiohttp.ClientTimeout(total=300 if path == '/Login/AutoHeartBeat' else 30)
        try:
            async with session.post(
                f'{base_url}{path}',
                params=query,
                json=body,
                data=form,
                timeout=timeout,
            ) as response:
                elapsed_ms = round((time.monotonic() - debug_started_at) * 1000) if debug_started_at is not None else None
                if response.status != 200:
                    error = await response.text()
                    await report_debug(
                        'C',
                        'AutoHeartBeat returned non-200',
                        {'status': response.status, 'elapsed_ms': elapsed_ms, 'response_text': error[:1000]},
                    )
                    raise Exception(f'wechat08 api {path} failed: {error}')
                result = await response.json()
                await report_debug(
                    'B',
                    'AutoHeartBeat response received',
                    {
                        'status': response.status,
                        'elapsed_ms': elapsed_ms,
                        'success': result.get('Success'),
                        'code': result.get('Code'),
                        'message': result.get('Message'),
                        'data_keys': list(result.get('Data', {}).keys()) if isinstance(result.get('Data'), dict) else None,
                    },
                )
                return result
        except Exception as e:
            await report_debug(
                'A',
                'AutoHeartBeat request raised exception',
                {
                    'elapsed_ms': round((time.monotonic() - debug_started_at) * 1000) if debug_started_at is not None else None,
                    'exception_type': type(e).__name__,
                    'exception_text': str(e),
                },
            )
            raise

    async def _ensure_login(self) -> str:
        """Login and return the bot wxid, following the XYBot login flow."""
        state = self._load_state()
        wxid = (self.config.get('wxid') or state.get('wxid') or '').strip()
        device_id = (self.config.get('device_id') or state.get('device_id') or '').strip()
        device_name = (self.config.get('device_name') or state.get('device_name') or '').strip()

        while not await self._is_logged_in(wxid):
            try:
                cached = await self._api_post('/Login/GetCacheInfo', form={'wxid': wxid}) if wxid else {}
                if cached.get('Success'):
                    await self.logger.info('尝试唤醒登录')
                    awaken = await self._api_post('/Login/LoginAwaken', body={'Wxid': wxid})
                    if not awaken.get('Success'):
                        raise Exception(f'唤醒登录失败: {awaken.get("Message")}')
                    uuid = (awaken.get('Data') or {}).get('Uuid') or ''
                    if not uuid:
                        raise Exception(f'唤醒登录失败: {awaken}')
                    await self.logger.info(f'获取到登录uuid: {uuid}')
                    login_hint = '请在手机微信上确认登录'
                else:
                    await self.logger.info('二维码登录')
                    if not device_name:
                        device_name = self._create_device_name()
                    if not device_id:
                        device_id = self._create_device_id()
                    uuid, login_hint = await self._request_qr_login(device_id, device_name)
            except Exception as e:
                await self.logger.warning(f'登录准备失败，回退到二维码登录: {e}')
                if not device_name:
                    device_name = self._create_device_name()
                if not device_id:
                    device_id = self._create_device_id()
                uuid, login_hint = await self._request_qr_login(device_id, device_name)

            wxid = await self._poll_login(uuid, login_hint)

        await self._save_state(wxid, device_id, device_name)
        await self.logger.info(f'登录成功 wxid: {wxid}')
        await self._start(wxid)
        return wxid

    async def _request_qr_login(self, device_id: str, device_name: str) -> tuple[str, str]:
        result = await self._api_post(
            '/Login/LoginGetQR',
            body={'DeviceID': device_id, 'DeviceName': device_name},
        )
        if not result.get('Success'):
            raise Exception(f'获取登录二维码失败: {result.get("Message")}')
        data = result.get('Data') or {}
        qr_base64 = data.get('QrBase64') or ''
        qr_url = data.get('QrUrl') or ''
        uuid = data.get('Uuid') or ''
        if not qr_base64 or not uuid:
            raise Exception(f'获取登录二维码失败: {result}')

        qr_file_path = ''
        try:
            qr_bytes = base64.b64decode(qr_base64.split(',', 1)[-1])
            qr_file_path = '/app/data/bot_log_images/wechat08_qr.jpg'
            os.makedirs(os.path.dirname(qr_file_path), exist_ok=True)
            with open(qr_file_path, 'wb') as f:
                f.write(qr_bytes)
        except Exception as e:
            await self.logger.warning(f'保存二维码图片失败: {e}')

        login_hint = '请使用微信扫描二维码登录（5分钟内有效）'
        if qr_file_path:
            login_hint += f'。二维码图片: {qr_file_path}'
        if qr_url:
            login_hint += f'；备用链接(浏览器打开): {qr_url}'
        await self.logger.info(login_hint, images=[platform_message.Image(base64=qr_base64)])
        return uuid, login_hint

    async def _is_logged_in(self, wxid: str) -> bool:
        """Check whether the backend still has a usable login session."""
        if not wxid:
            return False
        try:
            profile = await self._api_post('/User/GetContractProfile', form={'wxid': wxid})
            return bool(profile.get('Success'))
        except Exception:
            return False

    @staticmethod
    def _create_device_name() -> str:
        first_names = [
            'Oliver', 'Emma', 'Liam', 'Ava', 'Noah', 'Sophia', 'Elijah', 'Isabella',
            'James', 'Mia', 'William', 'Amelia', 'Benjamin', 'Harper', 'Lucas', 'Evelyn',
            'Henry', 'Abigail', 'Alexander', 'Ella', 'Jackson', 'Scarlett', 'Sebastian',
            'Grace', 'Aiden', 'Chloe', 'Matthew', 'Zoey', 'Samuel', 'Lily', 'David',
            'Aria', 'Joseph', 'Riley', 'Carter', 'Nora', 'Owen', 'Luna', 'Daniel',
            'Sofia', 'Gabriel', 'Ellie', 'Matthew', 'Avery', 'Isaac', 'Mila', 'Leo',
            'Julian', 'Layla',
        ]
        last_names = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
            'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
            'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
            'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
            'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill',
            'Flores', 'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell',
            'Mitchell', 'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans',
        ]
        return f"{random.choice(first_names)} {random.choice(last_names)}'s Pad"

    @staticmethod
    def _create_device_id() -> str:
        source = ''.join(random.choice(string.ascii_letters) for _ in range(15))
        return '49' + hashlib.md5(source.encode()).hexdigest()[2:]

    async def _poll_login(self, uuid: str, hint: str) -> str:
        """Poll /Login/LoginCheckQR until the full login response is returned."""
        deadline = asyncio.get_event_loop().time() + QR_POLL_TIMEOUT
        last_status = None
        while True:
            await asyncio.sleep(QR_POLL_INTERVAL)
            if asyncio.get_event_loop().time() > deadline:
                await self.logger.error('等待登录确认超时（5分钟），请重新启用机器人')
                raise Exception('等待扫码超时')
            try:
                check = await self._api_post('/Login/LoginCheckQR', form={'uuid': uuid})
            except Exception as e:
                await self.logger.warning(f'检测扫码状态失败: {e}')
                continue
            if check.get('Code') == -3:
                verify_url = ''
                verify_data = check.get('Data')
                if isinstance(verify_data, dict):
                    verify_url = verify_data.get('url') or ''
                if verify_url:
                    await self.logger.info(f'需要人脸/滑块验证，请在浏览器打开: {verify_url}')
                continue
            if not check.get('Success'):
                state_desc = f"Code={check.get('Code')} Message={check.get('Message')}"
                if state_desc != last_status:
                    await self.logger.debug(f'登录状态: {state_desc}')
                    last_status = state_desc
                continue

            data = check.get('Data') or {}
            if not isinstance(data, dict):
                continue
            acct = data.get('acctSectResp') or {}
            wxid = acct.get('userName') or ''
            if wxid:
                await self.logger.info(f'登录成功 wxid: {wxid}')
                return wxid
            state_desc = f"status={data.get('status')} expiredTime={data.get('expiredTime')}"
            if state_desc != last_status:
                await self.logger.debug(f'登录状态: {state_desc}')
                last_status = state_desc

        raise Exception('登录失败')

    def _load_state(self) -> dict:
        """Load persisted login state (wxid/device_id) from the data dir."""
        try:
            with open(STATE_FILE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    async def _save_state(self, wxid: str, device_id: str, device_name: str = 'Mac'):
        """Persist login state so restarts can reuse the login without scanning."""
        try:
            os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
            with open(STATE_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump({'wxid': wxid, 'device_id': device_id, 'device_name': device_name}, f, ensure_ascii=False)
            await self.logger.debug(f'登录状态已保存: wxid={wxid}')
        except Exception as e:
            await self.logger.warning(f'保存登录状态失败: {e}')

    async def _handle_message(self, message: platform_message.MessageChain, target_id: str):
        """Send a MessageChain to target_id."""
        content_list = await self.message_converter.yiri2target(message)
        at_targets = [item['target'] for item in content_list if item['type'] == 'at']
        at_targets = [t for t in at_targets if t != 'all']

        member_name_map: dict[str, str] = {}
        if at_targets:
            try:
                result = await self._api_post(
                    '/Group/GetChatRoomMemberDetail',
                    body={'QID': target_id, 'Wxid': self.config['wxid']},
                )
                member_data = ((result.get('Data') or {}).get('NewChatroomData') or {}).get('ChatRoomMember') or []
                for member in member_data:
                    user_name = member.get('UserName') or ''
                    if user_name in at_targets:
                        member_name_map[user_name] = member.get('NickName') or member.get('DisplayName') or user_name
                await self.logger.debug(f'群@成员昵称: {member_name_map}')
            except Exception as e:
                await self.logger.warning(f'获取群成员昵称失败: {e}')

        for msg in content_list:
            handler_map = {
                'text': lambda m: self._send_text(target_id, m['content'], at_targets, member_name_map),
                'image': lambda m: self._send_image(target_id, m['base64']),
                'file': lambda m: self._send_file(target_id, m['base64'], m['name']),
                'voice': lambda m: self._send_voice(target_id, m['base64'], m['duration']),
                'WeChatEmoji': lambda m: self._send_emoji(target_id, m['emoji_md5'], m['emoji_size']),
                'WeChatAppMsg': lambda m: self._send_app(target_id, m['app_msg']),
                'at': lambda m: None,
            }
            handler = handler_map.get(msg['type'])
            if handler is None:
                await self.logger.warning(f'未处理的消息类型: {msg["type"]}')
                continue
            try:
                await handler(msg)
                await self.logger.debug(f'消息发送成功 type={msg["type"]} target={target_id}')
            except Exception as e:
                await self.logger.error(f'发送消息失败 ({msg["type"]}) target={target_id}: {e}')

    async def _send_text(self, target_id: str, content: str, at_targets: list[str], member_name_map: dict):
        """Send a text message; at_targets are wxids for group @."""
        at_str = ','.join(at_targets)
        if at_targets:
            at_names = ' '.join(f'@{member_name_map.get(t, t)}' for t in at_targets)
            content = f'{at_names} {content}'
        result = await self._api_post(
            '/Msg/SendTxt',
            body={
                'Wxid': self.config['wxid'],
                'ToWxid': target_id,
                'Content': content,
                'Type': 1,
                'At': at_str,
            },
        )
        if not result.get('Success'):
            raise Exception(result.get('Message'))

    async def _send_image(self, target_id: str, base64_data: str):
        """Send an image; strip any data: URI prefix."""
        base64_data = base64_data.split(',', 1)[-1] if ',' in base64_data and base64_data.split(',', 1)[0].startswith('data:') else base64_data
        result = await self._api_post(
            '/Msg/UploadImg',
            body={'Wxid': self.config['wxid'], 'ToWxid': target_id, 'Base64': base64_data},
        )
        if not result.get('Success'):
            raise Exception(result.get('Message'))

    async def _send_file(self, target_id: str, base64_data: str, file_name: str):
        """Send a file: upload via /Tools/UploadFile, then send an appmsg (type 6) via /Msg/SendApp.

        The appmsg XML must use the compact client format (no <content>ok{@cdn...}</content>
        node, showtype=0, and an <md5> of the raw file bytes); the iOS-style short XML is
        rejected by the backend with BaseResponse.ret=-2.
        """
        base64_data = (
            base64_data.split(',', 1)[-1]
            if ',' in base64_data and base64_data.split(',', 1)[0].startswith('data:')
            else base64_data
        )
        upload_result = await self._api_post(
            '/Tools/UploadFile',
            body={'Wxid': self.config['wxid'], 'Base64': base64_data},
        )
        if not upload_result.get('Success'):
            raise Exception(upload_result.get('Message'))
        upload_data = upload_result.get('Data') or {}
        media_id = upload_data.get('mediaId') or ''
        total_len = int(upload_data.get('totalLen') or 0)
        if not media_id:
            raise Exception('file upload returned no mediaId')

        file_bytes = base64.b64decode(base64_data)
        file_md5 = hashlib.md5(file_bytes).hexdigest()

        file_name = file_name or 'file'
        file_ext = file_name.rsplit('.', 1)[-1].lower() if '.' in file_name else ''
        file_ext = re.sub(r'[^a-z0-9]', '', file_ext)[:8] or 'file'
        appmsg_xml = (
            '<appmsg appid="" sdkver="0">'
            f'<title>{xml_escape(file_name)}</title>'
            '<des></des><type>6</type><showtype>0</showtype><soundtype>0</soundtype>'
            '<contentattr>0</contentattr><appattach>'
            f'<totallen>{total_len}</totallen>'
            f'<attachid>{media_id}</attachid>'
            f'<fileext>{file_ext}</fileext>'
            '<emoticonmd5></emoticonmd5><cdnattachurl></cdnattachurl>'
            '<cdnthumbaeskey></cdnthumbaeskey><aeskey></aeskey><encryver>0</encryver>'
            '<filekey></filekey><overwrite_newmsgid></overwrite_newmsgid>'
            '<fileuploadtoken></fileuploadtoken></appattach>'
            f'<md5>{file_md5}</md5>'
            '<recorditem></recorditem></appmsg>'
        )
        result = await self._api_post(
            '/Msg/SendApp',
            body={'Wxid': self.config['wxid'], 'ToWxid': target_id, 'Type': 6, 'Xml': appmsg_xml},
        )
        if not result.get('Success'):
            raise Exception(result.get('Message'))
        base_response = (result.get('Data') or {}).get('BaseResponse') or {}
        if base_response.get('ret') not in (None, 0):
            raise Exception(f"send app message rejected: ret={base_response.get('ret')}")

    async def _send_voice(self, target_id: str, base64_data: str, duration: int):
        """Send a voice message (SILK, duration in milliseconds)."""
        base64_data = base64_data.split(',', 1)[-1] if ',' in base64_data and base64_data.split(',', 1)[0].startswith('data:') else base64_data
        result = await self._api_post(
            '/Msg/SendVoice',
            body={
                'Wxid': self.config['wxid'],
                'ToWxid': target_id,
                'Base64': base64_data,
                'Type': 4,  # SILK
                'VoiceTime': int(duration or 0) * 1000,  # SDK length is seconds; API expects milliseconds
            },
        )
        if not result.get('Success'):
            raise Exception(result.get('Message'))

    async def _send_emoji(self, target_id: str, emoji_md5: str, emoji_size: int):
        result = await self._api_post(
            '/Msg/SendEmoji',
            body={
                'Wxid': self.config['wxid'],
                'ToWxid': target_id,
                'Md5': emoji_md5,
                'TotalLen': int(emoji_size or 0),
            },
        )
        if not result.get('Success'):
            raise Exception(result.get('Message'))

    async def _send_app(self, target_id: str, app_msg: str):
        result = await self._api_post(
            '/Msg/SendApp',
            body={'Wxid': self.config['wxid'], 'ToWxid': target_id, 'Type': 0, 'Xml': app_msg},
        )
        if not result.get('Success'):
            raise Exception(result.get('Message'))

    async def send_message(self, target_type: str, target_id: str, message: platform_message.MessageChain):
        return await self._handle_message(message, target_id)

    async def reply_message(
        self,
        message_source: platform_events.MessageEvent,
        message: platform_message.MessageChain,
        quote_origin: bool = False,
    ):
        if message_source.source_platform_object:
            target_id = (message_source.source_platform_object.get('FromUserName') or {}).get('string', '')
            return await self._handle_message(message, target_id)

    async def is_muted(self, group_id: int) -> bool:
        pass

    def register_listener(
        self,
        event_type: typing.Type[platform_events.Event],
        callback: typing.Callable[
            [platform_events.Event, abstract_platform_adapter.AbstractMessagePlatformAdapter], None
        ],
    ):
        self.listeners[event_type] = callback

    def unregister_listener(
        self,
        event_type: typing.Type[platform_events.Event],
        callback: typing.Callable[
            [platform_events.Event, abstract_platform_adapter.AbstractMessagePlatformAdapter], None
        ],
    ):
        pass

    async def run_async(self):
        # SDK entry point (called by the platform runtime to start the adapter).
        # It only triggers the login flow; _start() is invoked by the login
        # flow itself once login succeeds (cache / wake-up / QR).
        self.config['base_url'] = (self.config.get('base_url') or '').rstrip('/') or 'http://127.0.0.1:8062/api'
        self.config['callback_port'] = int(self.config.get('callback_port') or DEFAULT_CALLBACK_PORT)

        if str(self.config.get('clear_cache', '')).lower() in ('true', '1', 'yes'):
            if os.path.exists(CLEAR_CACHE_FLAG_PATH):
                await self.logger.info(
                    'clear_cache 已执行过一次，本次启动跳过清理。'
                    f'如需再次清理，请删除容器内 {CLEAR_CACHE_FLAG_PATH} 后再重启'
                )
            else:
                await self._clear_cache()

        await self._ensure_login()

    async def _clear_cache(self):
        """Clear the persisted login cache once so the next start performs a fresh QR login.

        Only the LangBot-side cache is removed (the local state file); the backend
        session data is left untouched. Useful when switching to a new account. A
        one-shot flag is written so this only happens once even if the config
        option stays enabled.
        """
        try:
            if os.path.exists(STATE_FILE_PATH):
                os.remove(STATE_FILE_PATH)
                await self.logger.info(f'已清除 wechat08 登录缓存: {STATE_FILE_PATH}')
            else:
                await self.logger.info('wechat08 登录缓存不存在，无需清除')
            with open(CLEAR_CACHE_FLAG_PATH, 'w', encoding='utf-8') as f:
                f.write(time.strftime('%Y-%m-%d %H:%M:%S'))
            await self.logger.info(f'已写入一次性清理标记: {CLEAR_CACHE_FLAG_PATH}')
            self.config['wxid'] = ''
        except Exception as e:
            await self.logger.warning(f'清除 wechat08 登录缓存失败: {e}')

    async def _start(self, wxid: str):
        """Start the heartbeat and the message push callback server.

        Called by the login flow right after a successful login.
        """
        self.config['wxid'] = wxid
        self.bot_account_id = wxid

        # Start the backend heartbeat and message push loop. The server-side
        # handler may block retrying the heartbeat send, so never fail startup.
        try:
            await self.logger.info(f'开启自动心跳 (wxid: {wxid})')
            heartbeat_result = await self._api_post('/Login/AutoHeartBeat', form={'wxid': wxid})
            if not heartbeat_result.get('Success'):
                await self.logger.warning(
                    f'开启自动心跳失败: {heartbeat_result.get("Message")}，服务端会在后台自动重试'
                )
            else:
                await self.logger.info(f'自动心跳与消息推送已开启 (wxid: {wxid})')
        except Exception as e:
            await self.logger.warning(f'开启自动心跳超时或异常，已跳过（服务端会在后台自动重试）: {e}')

        await self.logger.info(
            f'wechat08 登录成功 wxid: {wxid}；消息回调监听端口: {self.config["callback_port"]}。'
            '请确保 wechatReal08 服务端 conf/app.conf 中 msgpush=true，'
            '且 syncmessagebusinessuri 指向本回调地址（Docker 内请用宿主机地址，如 '
            f'http://host.docker.internal:{self.config["callback_port"]}/msg/SyncMessage/{{0}}）'
        )

        async def shutdown_trigger_placeholder():
            while True:
                await asyncio.sleep(1)

        await self.logger.info(f'消息回调服务已启动: 0.0.0.0:{self.config["callback_port"]}/msg/SyncMessage/{wxid}')
        await self.quart_app.run_task(
            host='0.0.0.0',
            port=self.config['callback_port'],
            shutdown_trigger=shutdown_trigger_placeholder,
        )

    async def kill(self) -> bool:
        try:
            if self.config.get('wxid'):
                await self.logger.info(f'wechat08 适配器停止，关闭自动心跳 (wxid: {self.config["wxid"]})')
                await self._api_post('/Login/CloseAutoHeartBeat', query={'wxid': self.config['wxid']})
        except Exception:
            pass
        return True
