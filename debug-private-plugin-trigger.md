# Debug Session: private-plugin-trigger
- **Status**: [OPEN]
- **Issue**: Installed plugin `Typer_Body/GoodNewsGenerator` does not respond to private messages.
- **Log File**: `.dbg/trae-debug-log-private-plugin-trigger.ndjson`

## Reproduction Steps
1. Install and enable `Typer_Body/GoodNewsGenerator`.
2. Send a private message to the WeChat account.
3. Observe plugin execution and reply logs.

## Hypotheses & Verification
| ID | Hypothesis | Status | Evidence |
|----|------------|--------|----------|
| A | The plugin did not load or failed during initialization | Pending | Pending |
| B | The plugin requires a specific command/keyword and ordinary private messages do not match | Pending | Pending |
| C | The private-message event does not reach LangBot's pipeline/event converter | Pending | Pending |
| D | The plugin runs but sending the reply fails due to target/session/permission handling | Pending | Pending |

## Instrumentation Plan
- Inspect the plugin manifest and handler/trigger declarations.
- Trace existing runtime logs for plugin load, message receipt, matching, invocation, and reply errors.
- Add only minimal runtime instrumentation if static evidence is insufficient.

## Log Evidence
- Plugin installation is persisted and enabled: `plugin_settings` contains `Typer_Body / GoodNewsGenerator / enabled=1`.
- The plugin registers both private and group message events, so private chat is supported.
- The plugin only matches `^喜报\\s+(.+)$` or `^悲报\\s+(.+)$`; ordinary messages are intentionally ignored.
- The bot record has `use_pipeline_uuid = NULL` and `pipeline_routing_rules = []`.
- The only pipeline (`test`, UUID `8cc878df-6368-4688-9f92-c6b50faa23df`) has `is_default = 0`.
- LangBot logs contain `No pipeline_uuid for query 2/3, query dropped`, which means the message is discarded before the pipeline and plugin event dispatch.
- The pipeline's `extensions_preferences.enable_all_plugins` is true, so plugin binding is not the current blocker once a pipeline is selected.

## Hypotheses & Verification
| ID | Hypothesis | Status | Evidence |
|----|------------|--------|----------|
| A | The plugin did not load or failed during initialization | Rejected as primary cause | Plugin is installed/enabled; no private-chat restriction was found |
| B | The plugin requires a specific command/keyword | Confirmed as a secondary condition | Only `喜报 内容` / `悲报 内容` match |
| C | The private-message event does not reach LangBot's pipeline/event chain | Confirmed | Bot has no pipeline; logs show queries dropped for missing `pipeline_uuid` |
| D | The plugin runs but sending the reply fails | Rejected for current symptom | Message is dropped before plugin invocation |

## Fix Plan
1. Bind bot `874` to pipeline `test`, or mark `test` as the default pipeline.
2. Restart/reload the bot if the runtime does not hot-reload bot settings.
3. Send exactly `喜报 测试` or `悲报 测试` in a private chat.

## Verification Conclusion
The plugin is not triggered because the bot has no effective pipeline, so private messages are discarded before plugin events. After assigning a pipeline, only the documented `喜报/悲报 + 空格 + 内容` format will trigger the plugin.
