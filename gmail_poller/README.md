# gmail_poller

Ductile plugin that polls Gmail for new messages using Gmail's History API and emits `gmail.new_message` signal events.

## Design

**Signal-only**: emits minimal metadata per message. Full content is fetched downstream by the pipeline consumer (e.g. a `k prompt` one-shot PAI session using `gws gmail users messages get`).

**historyId tracking**: uses Gmail's monotonic `historyId` rather than unread status. Robust against read/unread changes, external clients, and service restarts.

**Auth delegation**: no credentials in this plugin. Auth is owned by the `gws` CLI (`~/.config/gws/credentials.enc`).

## Plugin Facts

`poll` declares `gmail_poller.snapshot` as a fact output from `state_updates`.
Ductile records that snapshot append-only in `plugin_facts` and keeps
`plugin_state` as the compatibility/current-view row via `mirror_object`.

## Event Schema

```json
{
  "type": "gmail.new_message",
  "payload": {
    "message_id":  "19d456cf036d33a3",
    "thread_id":   "19d456cf036d33a3",
    "from":        "sender@example.com",
    "subject":     "Re: Hello",
    "snippet":     "Got it. Test successful...",
    "label_ids":   ["INBOX", "UNREAD"],
    "received_at": "Wed, 1 Apr 2026 06:45:32 +1100"
  },
  "dedupe_key": "gmail:msg:19d456cf036d33a3"
}
```

## Config Keys

| Key | Default | Description |
|-----|---------|-------------|
| `gws_binary` | `gws` | Path or name of the `gws` executable |
| `label_filter` | `INBOX` | Gmail label ID to watch |
| `max_per_poll` | `20` | Maximum events emitted per tick |
| `emit_on_first_run` | `false` | Reserved — first run always establishes baseline only |

## Ductile Config

```yaml
plugins:
  gmail_poller:
    enabled: true
    schedules:
      - every: 5m
        if_running: skip
        catch_up: skip
        timezone: Australia/Melbourne
    config:
      gws_binary: gws
      label_filter: INBOX
      max_per_poll: 20
    retry:
      max_attempts: 3
      backoff_base: 30s
    timeouts:
      poll: 30s
      health: 10s
    circuit_breaker:
      threshold: 5
      reset_after: 30m
    max_outstanding_polls: 1
```

## Pipeline Example

```yaml
pipelines:
  - name: handle_new_email
    on: gmail.new_message
    steps:
      - uses: sys_exec
        with:
          command: "k prompt 'New email. id={{payload.message_id}} from={{payload.from}} subject={{payload.subject}} snippet={{payload.snippet}}. Fetch full content with gws and decide how to respond.'"
```

## Prerequisites

- `gws` CLI installed and authenticated (`gws auth status` shows `token_valid: true`)
- Gmail API enabled in the GCP project used for gws auth

## Failure Modes

| Condition | Status | Retry | Notes |
|-----------|--------|-------|-------|
| `gws` not in PATH | error | false | Fix config |
| Gmail 401/403 | error | false | Re-run `gws auth login` |
| Gmail 429 / 5xx | error | true | Transient; ductile retries |
| historyId stale (404) | ok | — | Self-heals; warns in logs |
| Empty poll | ok | — | Normal; no events emitted |
