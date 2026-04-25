# email_send

Send a single email via the [`gws`](https://github.com/...) CLI's
`gmail.users.messages.send` endpoint. Stateless plugin: no durable memory,
no `fact_outputs`. Each invocation is independent.

## Contract

### Commands

| Command | Type | Description |
|---|---|---|
| `send` | write | Send an email. Builds RFC822, base64url-encodes, POSTs via gws. Emits `email_send.sent`. |
| `health` | read | Verify gws + gmail auth via getProfile. Emits `email_send.health`. No state_updates. |

### Event payload — `send`

```yaml
to:         string  # required if config.default_to not set
subject:    string  # required
body_text:  string  # required
body_html:  string  # optional; if present sends multipart/alternative
dry_run:    bool    # optional; when true appends --dry-run to gws (validates locally, no API send)
```

### Emitted event — `email_send.sent`

```yaml
to:          string
subject:     string
message_id:  string  # gmail message id
thread_id:   string  # gmail thread id
sent_at:     string  # ISO8601
```

### Config

```yaml
plugins:
  email_send:
    enabled: true
    config:
      gws_binary: gws                # optional, default "gws"
      default_to: ""                 # optional fallback recipient
      default_dry_run: false         # optional; force all sends to be dry-run
      send_timeout_seconds: 30       # optional
      health_timeout_seconds: 15     # optional
```

All keys are optional. If the event payload omits `to`, `default_to` is used; if neither is set, the plugin returns a non-retryable error.

### Authentication

`gws` handles all Google Workspace auth — OAuth tokens are managed by `gws` itself, not this plugin. If `health` returns an auth error, run `gws auth login` (or your equivalent re-auth flow) on the host running ductile.

### Idempotency

`send` is **not idempotent**: retrying after a successful POST sends a duplicate. `retry_safe` is `false` to bias the runtime toward not retrying ambiguous failures.

If you need send-once-and-only-once, use a `dedupe_key` on the upstream event and a deduplication step in the pipeline.

## Example pipeline

```yaml
pipelines:
  - name: weekly-email
    on: health_weekly_report.assembled
    steps:
      - id: send
        uses: email_send
        with:
          to: matt.example@gmail.com
          subject: "{payload.subject}"
          body_text: "{payload.body_text}"
          body_html: "{payload.body_html}"
```

## Notes

- `concurrency_safe: true` — no shared local resource; multiple sends in flight are fine if the upstream pipeline allows it.
- No `state_updates` from any command (per ductile §5 for `health`, and minimal-pattern for `send`).
- Action provenance (which message_id was sent when, to whom) lives in `job_log` automatically — not duplicated into plugin state.
