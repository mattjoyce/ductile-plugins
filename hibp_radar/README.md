# hibp_radar

Monitor configured email identities and owned domains for new appearances in HIBP (Have I Been Pwned) breach dumps. Emits one event per *new* breach name — silent on already-seen breaches.

## User Story

**As** an operator with multiple email aliases and one or more owned domains,
**I want** ductile to watch every address and domain for new appearances in HIBP breach dumps,
**so that** the moment one of my identities lands in a fresh dump, I get a delta event with the breach name and the data classes — instead of finding out months later through someone else's attack.

### Why

- Knowing *when* a fresh dump publishes one of my addresses converts a static credential-rotation list into a live priority queue.
- Manual checking on the HIBP website doesn't scale across multiple aliases plus owned domains.
- Ductile already has notifier plugins. Adding one more publisher to that pipe is almost free.

### What

Three commands:

- `poll` — for each configured identity, query `breachedaccount`; for each configured domain, query `breachedomain`. Emits `hibp.new_breach` events only for breach names not previously seen.
- `handle` — accepts an HIBP domain-subscription push webhook payload and emits an event if the breach is new for that domain.
- `health` — reports per-surface readiness (paid API key configured? identities/domains configured?).

### How

- Python uv-script. Protocol v2.
- Durable state via `state_updates` recorded as `hibp_radar.snapshot` facts (see manifest `fact_outputs`). Identities are keyed by SHA-256 of lowercased email — **plaintext email is never persisted in state or written to logs**.
- Logs and events surface the configured `label` only.
- Rate limited via `rate_limit_ms` config (default 1600ms) — HIBP requires ≥1500ms spacing on lower paid tiers.
- `concurrency_safe: false` — serialised invocations to honour the rate-limit window.

## Endpoints used

- `GET /api/v3/breachedaccount/{email}?truncateResponse=false` — paid (HIBP Pwned 1+ tier).
- `GET /api/v3/breachedomain/{domain}` — free for domains the API key holder has verified ownership of via DNS TXT record.

Stealerlogs is deliberately out of scope for v0.1.0 (requires a higher-tier paid HIBP plan).

## Config

All keys optional. The plugin degrades gracefully when surfaces are unconfigured:

- No `hibp_api_key` → poll skips all surfaces, returns ok with a warn log, preserves prior state.
- Empty `identities` → no per-account queries, domains still polled if configured.
- Empty `domains` → no domain queries, identities still polled if configured.

```yaml
plugins:
  hibp_radar:
    hibp_api_key: ${HIBP_API_KEY}      # required for any actual queries
    user_agent_email: you@example.com  # contact email per HIBP UA policy
    rate_limit_ms: 1600                # default; ≥1500 for HIBP lower tiers
    timeout_seconds: 20

    identities:
      - { label: alias-1, email: alias1@example.com }
      - { label: alias-2, email: alias2@example.com }
      - { label: alias-3, email: alias3@example.org }
      # … add as many as you like; each consumes one HIBP request per poll

    domains:
      - example.com    # must be HIBP-verified via DNS TXT first
      - example.org    # must be HIBP-verified via DNS TXT first
```

Pick `label` values you'll recognise at a glance — they're what appears in events and logs. Avoid leaking address structure into the label.

## Pipeline wiring (suggested)

A pipeline that pipes deltas into a notifier:

```yaml
pipelines:
  hibp_breach_alert:
    on_event: hibp.new_breach
    steps:
      - plugin: <your_notifier_plugin>
        command: handle
        params:
          template: |
            New breach hit on {{ payload.surface }} `{{ payload.label }}`
            Breach: {{ payload.breach_name }} ({{ payload.breach_date }})
            Data classes: {{ payload.data_classes | join(', ') }}
            Action: {{ payload.recommended }}
```

## Event shape

```json
{
  "type": "hibp.new_breach",
  "payload": {
    "surface": "identity",
    "label": "alias-1",
    "breach_name": "Adobe",
    "breach_date": "2013-10-04",
    "data_classes": ["Email addresses", "Password hints", "Passwords"],
    "recommended": "Rotate credential for any service tied to alias-1; check if alias-1 is recovery on critical accounts and detach."
  },
  "dedupe_key": "hibp:identity:<sha256>:Adobe"
}
```

## Snapshot shape (`state_updates` → `hibp_radar.snapshot`)

The snapshot is presence-stable: top-level `identities` and `domains` keys are present on every invocation. Per-identity entries always carry `label`, `breaches_seen`, `last_polled_at`. `breaches_seen` lists are deterministically sorted.

```json
{
  "identities": {
    "<sha256_lowercased_email>": {
      "label": "alias-1",
      "breaches_seen": ["Adobe", "LinkedIn"],
      "last_polled_at": "2026-04-27T01:23:45+00:00"
    }
  },
  "domains": {
    "example.com": {
      "breaches_seen": ["<alias_hash>|Adobe"],
      "last_polled_at": "2026-04-27T01:23:45+00:00"
    }
  }
}
```

## Privacy posture

- Plaintext email never written to state files, never written to logs.
- State key is SHA-256 of the lowercased email (not reversible without a guessing attack against a known address).
- Logs and emitted events surface the configured `label` (e.g. `alias-1`), not the address.
- Domain state stores `breaches_seen` keys as `<sha256_first16_of_alias>|<breach_name>` — the alias local-part is hashed so repository inspection of the snapshot does not reveal which addresses on the domain were hit.
- API key lives in ductile's secrets store, mode `0600`, gitignored. Reference via `${ENV_VAR}` interpolation.

## Health output

```
hibp_radar: paid_api=ready (3 identities, 2 domains)
```

or, when no key is configured:

```
hibp_radar: paid_api=DISABLED (no hibp_api_key configured)
```

## Operator handoff (one-time setup)

1. **HIBP API key** — sign up at <https://haveibeenpwned.com/API/Key>. The Pwned 1 tier covers `breachedaccount` and `breachedomain`.
2. **Domain ownership verification** — for each domain you want to monitor, the HIBP key dashboard issues a DNS TXT token. Add it to your DNS provider, then verify in the dashboard.
3. **Plugin config** — add a `plugins.hibp_radar` block to your ductile config (sample above). Use `${HIBP_API_KEY}` to interpolate the secret.
4. **Schedule poll** at a 6h cadence (sufficient given HIBP's update tempo and the rate-limit window).
5. **Wire pipeline** to your notifier of choice (sample above).
6. **Verify**: `ductile plugin handle hibp_radar health` → expect `paid_api=ready (...)`.

## Tests

```bash
cd hibp_radar/
python3 -m unittest test_run -v
```
