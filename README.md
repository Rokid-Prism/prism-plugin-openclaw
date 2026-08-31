# OpenClaw Native PluginBridge Plugin

This is a normal PluginBridge plugin package. It can be installed by copying the
whole `openclaw` directory into a Prism Hub `plugin_dirs` entry, or by
letting Hub install the packaged version into
`~/.prism/pluginbridge-plugins/openclaw`.

The plugin speaks Prism Hub's JSON-over-stdio PluginBridge protocol on one side
and OpenClaw Gateway WebSocket RPC on the other side.

Its Prism runtime mode is `native`. `gateway` is only the underlying OpenClaw
transport and is not exposed as a user-selectable Prism runtime.

Capabilities:

- Forward: create/resume a native-visible OpenClaw Gateway session, send Prism
  messages into it, subscribe run/message progress, and verify visibility via
  chat history.
- Reverse: expose `ListSessions` / `AttachSession` / `Subscribe` so Hub can
  synchronize the Gateway session directory and activity through one plugin-wide
  subscription rather than one WebSocket per conversation.
- Controls: read and switch model/reasoning, report context usage, interrupt,
  compact, rename, pin, archive, delete, and resolve native exec/plugin approval
  requests. Permission switching and queue controls are not published because
  the installed Gateway does not expose a stable per-session permission state
  or queue contract.
- Attachments: pass Hub-materialized files to `chat.send` as base64 payloads.
  Prism rejects files above OpenClaw's default 20 MiB media limit before reading
  them; the Gateway remains authoritative for configured and media-type limits.

Product note:

- `ListSessions`, `AttachSession`, `Subscribe`, `WaitForRun`, and visibility
  verification are the minimum set for a full mobile/glasses remote-conversation
  plugin.
- Reverse sync is useful but not the key gate.
- Gateway-connected TUI, Control UI, channels, and Prism are concurrent peers;
  the Gateway broadcasts live session/message/run/approval events and keeps one
  authoritative history. Embedded local commands such as `tui --local`,
  `chat`, or `terminal` bypass that live bus and are not a full realtime path.

Official direction:

- keep `openclaw` as the formal protocol-native sample path
- do not migrate OpenClaw to desktop automation
- keep extending native history/message query integration instead of changing the integration mode

Repo development only:

```sh
go test ./...
go build -o ./bin/prism-plugin-openclaw ./cmd/openclaw-adapter
```

The repository validates OpenClaw directly with Go tests; release packaging builds `./bin/prism-plugin-openclaw` from `cmd/openclaw-adapter`.
The runtime launcher never compiles source on the user's machine. It starts the
packaged binary directly and fails explicitly if that artifact is missing;
developers must run the build command above after changing Go source.

For normal product usage, plugin install / remove goes through Prism Desktop, not the public CLI surface.

The package is intentionally independent from Prism Hub internals. It imports
the public Go module `github.com/Rokid-Prism/prism-plugin-sdk`; forks should
use that released module rather than a local `replace` directive.
