# agent_handshake

Challenge-response barrier for autonomous agents. Validates a `SHA256(challenge + salt)` proof-of-work submitted by an agent and emits an `agent_handshake.registered` event so downstream pipelines (typically `discord_notify`) can announce the registration. Append-writes to an optional log file. Designed to be deployed as an **alias** with instance-specific `challenge`/`salt` config — see the `ap_canary` deploy below for the canonical pattern.

## Commands

- `handle` (write): Verify the submitted proof and register the agent. Appends to `config.log_path` if set, emits `agent_handshake.registered`, and increments the per-instance registration counter via `state_updates`.
- `health` (read): Verify the instance is configured with both `challenge` and `salt`. Does **not** validate that the configured proof would succeed — only that the required config keys are present and non-empty.

## Input Payload (`handle`)

| Field | Type | Required | Purpose |
|---|---|---|---|
| `email` | string | yes | Agent identity. Non-empty. |
| `challenge` | string | yes | Echo of the challenge string published to agents. Must match `config.challenge` exactly. |
| `proof` | string | yes | Hex `SHA256(challenge + salt)`. Must match the server-side computed proof. |
| `consent` | boolean | yes | Must be `true`. Any other value (`false`, missing, null) rejects the handshake. |
| `agent` | string | no | Optional agent self-identifier (e.g. `claude-opus-4-7`). Defaults to `unknown`. |

## Configuration

| Key | Required | Default | Purpose |
|---|---|---|---|
| `challenge` | **yes** | — | The challenge string this instance publishes. Each alias has its own value. |
| `salt` | **yes** | — | Secret salt used to compute the expected proof. Treat as a credential. |
| `log_path` | no | — | If set, appends a JSON line per successful registration: `{timestamp, email, agent, challenge}`. Log write failure is non-fatal. |

## Events

Emits `agent_handshake.registered` on successful proof verification, with payload: `email`, `agent`, `timestamp`, `challenge`, `message`, `text`.

## Durable state

On successful `handle`, returns `state_updates`:

```json
{
  "last_registration": "<iso-8601>",
  "total_registrations": <int>
}
```

Snapshotted as `agent_handshake.snapshot` via `fact_outputs` (mirror_object). The `PluginName` field of each fact captures the **instance** (e.g. `ap_canary`), so multiple aliases of `agent_handshake` get isolated counters keyed by alias name.

> **Note**: Without the `fact_outputs` rule the gateway discards `state_updates`, leaving `total_registrations` silently stuck at 1 for every registration — the counter increment in `run.py` reads back `0` every time. This was fixed in v0.2 of the manifest.

## Alias example (ap_canary on B450 prod)

```yaml
plugins:
  ap_canary:
    uses: agent_handshake
    enabled: true
    timeout: 10s
    max_attempts: 1
    config:
      challenge: "mattjoyce_ai_canary_2026"
      salt: "${AP_CANARY_SALT}"
      log_path: /app/data/ap_canary.log
```

And the pipeline that announces successful registrations:

```yaml
pipelines:
  - name: ap-canary-registered
    on: agent_handshake.registered
    steps:
      - id: notify
        uses: discord_notify
        with:
          message: "Agent registered: {payload.email} (agent: {payload.agent})"
          username: "AP Canary"
```

## Proof computation (for agents)

```python
import hashlib
proof = hashlib.sha256((challenge + salt).encode()).hexdigest()
```

Agents need the challenge (public) and the salt (issued out-of-band). POST:

```json
{
  "email": "agent@example.com",
  "challenge": "mattjoyce_ai_canary_2026",
  "proof": "<sha256-hex>",
  "consent": true,
  "agent": "claude-opus-4-7"
}
```
