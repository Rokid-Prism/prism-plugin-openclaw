# OpenClaw Gateway RPC 参考

本文件记录 Prism OpenClaw Native adapter 已在 OpenClaw `2026.7.1-2` 验证的 RPC。字段名以安装版本的 Gateway schema 与真实调用结果为准；不要从其他 Agent 协议猜测方法或字段。

## 会话与正文

| 方法 | Prism 用途 | 关键参数/返回 |
| --- | --- | --- |
| `gateway.identity.get` | 端点身份校验 | 连接和 token 认证后调用；失败即不可用 |
| `sessions.list` | 全量目录 | 请求 `{limit, offset?, includeGlobal:true, includeUnknown:true, configuredAgentsOnly:true, includeDerivedTitles:true}`，按 `hasMore/nextOffset` 读完；当前版本拒绝 `sortBy` |
| `sessions.create` | 新建会话 | 请求 `key/agentId/label/cwd?`，返回 `key/sessionId?` |
| `sessions.describe` | 会话详情 | 返回 session row，包括 model、thinking、context、标题、cwd 和状态 |
| `sessions.patch` | 模型、推理、标题、置顶、归档 | 请求 `key` 和对应幂等字段 |
| `sessions.delete` | 删除 | 请求 `{key}` |
| `sessions.compact` | 压缩上下文 | 请求 `{key}` |
| `chat.history` | 正文与发送可见性 | 请求 `sessionKey/limit/maxChars` |
| `chat.send` | 文本和附件发送 | 请求 `sessionKey/message/idempotencyKey/deliver:false/attachments?` |
| `chat.abort` | 打断 | 请求 `sessionKey/runId?` |
| `agent.wait` | run 终态与非阻塞状态 | `timeoutMs:0` 返回 timeout 表示仍运行，不是失败 |

`chat.send.attachments[]` 使用 `{type,mimeType,fileName,content}`，其中 `content` 是 base64。OpenClaw 的 `agents.defaults.mediaMaxMb` 默认值是 20 MiB；Prism 在读取文件前按 20 MiB 前置限制，Gateway 继续负责用户配置和媒体类型的最终校验。

会话目录的边界是同一个 OpenClaw Gateway/Profile 的 configured agent store。Web Control UI、TUI、微信等 channel 和 Prism 创建的会话，只要写入这套 Gateway store，都由同一个 `sessions.list` 返回；Plugin 不按 `origin.provider` 或 session key 前缀过滤。另一台 Gateway、另一个 `--profile` 或另一份 `OPENCLAW_STATE_DIR` 是独立数据源，不在当前 Plugin 实例内合并。

Plugin 保留 session row 的 `agentId/kind/category/spawnedBy/chatType/origin/archived/pinned/status` 分类信息，不在 adapter 入口丢弃 global、unknown、cron 或 spawned session。原生 `agentId/spawnedBy` 只用于 adapter 内部关联；统一目录只允许 `session_kind/category/chat_type/origin_*` 等语义分类字段进入公共 metadata，不能暴露原生 agent/session identity。普通目录仍由统一 Hub 投影过滤 `archived=true`；技术/后台会话是否默认折叠属于客户端展示策略，不能通过删除原生索引实现。相同 key 在分页竞争中重复出现时去重；`hasMore=true` 但 `nextOffset` 缺失、倒退或重复必须返回失败，禁止把不完整页发布为空或完整快照。

## 模型与推理

| 统一字段 | OpenClaw 来源 |
| --- | --- |
| `current_model` | `sessions.describe.session.model + modelProvider` |
| `model_options` | `models.list.models[]` 的 `id/provider/name`，过滤 `available:false` |
| `current_reasoning` | `thinkingLevel`，缺失时使用 `thinkingDefault` |
| `reasoning_options` | 当前 session row 的 `thinkingLevels[]`，并与 `models.list` 中同 provider/model 的 `reasoning` 交叉校验 |
| `context_tokens_used` | `sessions.describe.session.totalTokens` |
| `context_window_total` | `sessions.describe.session.contextTokens` |

切换前必须以当前 options 重验 target，然后调用 `sessions.patch {key, model}` 或 `{key, thinkingLevel}`，最后重新读取统一 detail。当前 OpenClaw 版本可能为 `reasoning:false` 的模型仍在 session row 返回通用 `thinkingLevels`；由于 `sessions.patch` 会拒绝这些档位，Plugin 必须在模型目录明确为 false 时隐藏 reasoning options 和切换 action。不能通过 slash command、显示文案或猜测默认档位实现。

## 订阅与审批

`sessions.subscribe` 建立 plugin-wide 订阅。单条 WebSocket 由一个 read loop 读取，按 request ID 分发 RPC response，并将异步 event 放入独立有界队列；不能在每个 request 中直接读取 socket，否则会丢掉交错到达的事件。

Gateway 支持多个 peer 同时连接和订阅。同一个 Gateway/Profile 下，Prism、Control UI、Gateway TUI 或 channel 任一侧发送后，其他 peer 都可收到 session/message/run/approval 事件并读取同一份 history。2026-08-17 的真实双客户端 E2E 已验证 peer -> Prism watcher 与 Prism -> peer history 两个方向。该结论不适用于 `openclaw tui --local`、`openclaw chat`、`openclaw terminal` 等绕过 Gateway live event bus 的嵌入式本地 runtime。

处理的事件包括：

- `sessions.changed`
- `session.message`
- `agent`
- `exec.approval.requested/resolved`
- `plugin.approval.requested/resolved`

审批使用 `exec.approval.list/resolve` 和 `plugin.approval.list/resolve`。连接建立后先读取两个 pending list，重放尚未处理的请求，再进入事件循环。Mobile 的 opaque action ID 携带 approval kind，回写到对应 resolver。不存在已验证的通用 `approval.get/resolve`。

## 明确不支持

- 不发布 permission。全局 exec approval policy 不能替代可回读的 per-session 当前权限。
- 不发布 queue。当前 Gateway 没有统一可读、可编辑、可取消的会话队列契约。
- 不调用 `commands.list` 来伪造 model/reasoning/action。
- 不发送当前安装版本拒绝的 `sessions.list.sortBy`，也不固定截断为最近 200 条。
- 不把 Gateway transport 名称暴露成 Prism runtime mode；对外固定为 `native`。
