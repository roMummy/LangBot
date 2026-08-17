# Debug Session: heartbeat-timeout
- **Status**: [OPEN]
- **Issue**: wechat08 登录成功后，调用 `/Login/AutoHeartBeat` 报告“开启自动心跳超时或异常，已跳过”，异常详情为空。
- **Debug Server**: Pending startup
- **Log File**: `.dbg/trae-debug-log-heartbeat-timeout.ndjson`

## Reproduction Steps
1. 启动 LangBot 与 wechatReal08 服务。
2. 使用 wechat08 完成登录或复用已有登录态。
3. 观察 `AutoHeartBeat` 调用及其返回/异常。

## Hypotheses & Verification
| ID | Hypothesis | Likelihood | Effort | Evidence |
|----|------------|------------|--------|----------|
| A | `/Login/AutoHeartBeat` 服务端调用本身超过客户端 30 秒超时，导致 `asyncio.TimeoutError`，所以异常文本为空 | High | Low | Pending |
| B | `wxid` 传参位置或格式不符合 wechatReal08 接口要求，服务端未获取到账号 | Medium | Low | Pending |
| C | 服务端因 Redis、登录缓存或消息回调初始化异常在接口内部重试/阻塞 | Medium | Medium | Pending |
| D | 账号的自动心跳已启动，服务端返回重复启动/运行中状态 | Low | Low | Pending |

## Instrumentation Plan
- A/B: 记录 `AutoHeartBeat` 请求开始、URL 参数、耗时、响应状态和响应 JSON。
- C/D: 记录非 200 响应、JSON 中的 `Code`、`Success`、`Message` 和异常类型/文本。

## Log Evidence
- Direct pre-fix request: `POST http://127.0.0.1:8062/api/Login/AutoHeartBeat?wxid=wxid_hjrazd32k8qm29` produced no response and timed out after 35 seconds.
- The server controller reads `wxid` and then synchronously calls `wXConnect.SendHeartBeat()` before writing its HTTP response.
- The server heartbeat implementation retries up to 3 times with a 175-second interval, so a failed first heartbeat can keep the HTTP request open well beyond 30 seconds.
- The Python adapter's pre-fix `_api_post` applied `aiohttp.ClientTimeout(total=30)` to this request, explaining the empty exception text (`asyncio.TimeoutError`).
- `bot.py` sends `wxid` as form data and uses `aiohttp.ClientSession()`'s 5-minute default total timeout.

## Hypotheses & Verification
| ID | Hypothesis | Status | Evidence |
|----|------------|--------|----------|
| A | `/Login/AutoHeartBeat` exceeds the adapter's 30-second client timeout | Confirmed | Direct request exceeded 35 seconds; server blocks in synchronous heartbeat/retry logic |
| B | `wxid` must be sent as form data to match the reference client | Supported | `bot.py` uses `data={"wxid": self.wxid}`; adapter used query params |
| C | Server-side heartbeat failure is the underlying reason for the long response | Not yet isolated | Requires wechatReal08 runtime logs or a successful post-fix response |
| D | The account's automatic heartbeat is already running | Rejected for this event | The request did not return a duplicate-running response; it timed out first |

## Post-fix Verification
- Code fix applied: `AutoHeartBeat` now sends `wxid` as form data and uses a 300-second total timeout, matching `bot.py`'s request behavior.
- Static diagnostics after the change show no new errors; only pre-existing unused-parameter hints remain.
- Runtime post-fix response is pending a LangBot restart/re-login because the currently running process loaded the previous adapter code.

## Verification Conclusion
The immediate warning is caused by the adapter's 30-second timeout, not by the login success itself. The request format and timeout are now aligned with `bot.py`; restart LangBot and verify that the log changes to `自动心跳与消息推送已开启` or exposes the actual server-side heartbeat error instead of an empty timeout.
