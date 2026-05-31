# discord_notify

Post a message to a Discord channel via an incoming webhook. Stateless writer — every invocation is an independent POST with no shared state, so the gateway may dispatch in parallel (`concurrency_safe: true`).

Reads the message body from `payload.message`, `payload.content`, `payload.title`, `payload.result` (in that order), with context fallback. Optional fallbacks: `config.message_template` (rendered against payload with `{field.path}` substitution) and `config.default_message`. Output is capped at Discord's 2000-char limit.

## Commands

- `handle` (write): Post a message to Discord.
- `poll` (write): Scheduled variant. Falls back to `config.poll_message` when the scheduler supplies no payload. Otherwise identical to `handle`.
- `health` (read): Validate that `webhook_url` is configured and looks like a Discord webhook URL. Does **not** post.

## Configuration

| Key | Required | Default | Purpose |
|---|---|---|---|
| `webhook_url` | **yes** | — | Discord incoming webhook URL. Required at config-lock time; without it the plugin is inert. |
| `default_username` | no | `Ductile` | Username for the webhook avatar/name. Overridable per-call via `payload.username`. |
| `default_avatar_url` | no | — | Avatar URL override for the webhook. |
| `default_message` | no | — | Body used when no `message`/`content`/`title`/`result` is supplied AND no `message_template` matches. Last-resort fallback before context.result. |
| `message_template` | no | — | Body template with `{field.path}` dot-notation substitution against the payload. Tried before `default_message`. |
| `poll_message` | no | — | Body used when `poll` runs with no scheduler payload. |
| `request_timeout_seconds` | no | 10 | HTTP timeout for the webhook POST. |

## Example

```yaml
plugins:
  discord_notify:
    enabled: true
    timeout: 15s
    retry:
      max_attempts: 2
    config:
      webhook_url: ${DISCORD_WEBHOOK_URL}
      default_username: "Ductile"
      poll_message: "Daily heartbeat"
```

Example payload:
```json
{ "message": "Deployment completed" }
```

## Resolution order

Body resolution (first non-empty wins):

1. `payload.message`
2. `payload.content`
3. `config.message_template` rendered against payload
4. `config.default_message`
5. `payload.result`

Title resolution: `payload.title`. Combined output: `**<title>**\n<body>` if both, otherwise whichever is present.
