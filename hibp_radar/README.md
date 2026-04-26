# hibp_radar

Monitor configured email identities and owned domains for new appearances in HIBP (Have I Been Pwned) breach dumps. Emits one event per *new* breach name — silent on already-seen breaches.

## User Story

**As** Matt, with multiple email aliases and owned domains,
**I want** ductile to watch every address and domain for new appearances in HIBP breach dumps,
**so that** the moment one of my identities lands in a fresh dump, I get a Discord ping with the breach name and the right next move — instead of finding out months later through someone else's attack.

### Why

- An attacker has already probed the surface (recovery-hook attempts, etc.) — knowing *when* a fresh dump publishes one of the addresses converts a static triage doc into a live priority queue.
- Manual checking on the HIBP website doesn't scale across multiple aliases plus owned domains.
- Discord is already wired into ductile via `discord_notify`. Adding one more publisher to the same alert pipe is almost free.

### What

Three commands:

- `poll` — for each configured identity, query `breachedaccount`; for each configured domain, query `breachedomain`. Emits `hibp.new_breach` events only for breach names not previously seen.
- `handle` — accepts an HIBP domain-subscription push webhook and emits an event if the breach is new for that domain.
- `health` — reports per-surface readiness (paid API key configured? identities/domains configured?).

### How

- Python uv-script (matches `gmail_poller`, `jina-reader`).
- State persisted via ductile's `state_updates`. No SQLite. Identities keyed by SHA-256 of lowercased email — **plaintext email is never stored or logged**. Logs reference the configured `label` only.
- Rate limited via `rate_limit_ms` config (default 1600ms) — HIBP requires ≥1500ms spacing on lower paid tiers.
- `concurrency_safe: false` in the manifest so ductile serialises invocations.

## Endpoints used

- `GET /api/v3/breachedaccount/{email}?truncateResponse=false` — paid (Pwned 1+).
- `GET /api/v3/breachedomain/{domain}` — free *for domains the API key holder has verified ownership of* via DNS TXT.

Stealerlogs is deliberately deferred — it requires a higher-tier paid HIBP plan.

## Config

All keys optional. The plugin degrades gracefully when surfaces are unconfigured.

```yaml
plugins:
  hibp_radar:
    hibp_api_key: <your HIBP v3 key>      # required for any actual queries
    user_agent_email: matt@lostplot.com   # contact email per HIBP UA policy
    rate_limit_ms: 1600                   # default; ≥1500 for HIBP lower tiers
    timeout_seconds: 20

    identities:
      - { label: gmail-primary,    email: matt.joyce@gmail.com }
      - { label: lostplot-primary, email: matt@lostplot.com }
      - { label: matholio-old,     email: matholio@gmail.com }
      - { label: gmail-no-dot,     email: mattjoyce@gmail.com }
      - { label: gmail-77,         email: matt.joyce77@gmail.com }
      - { label: proton-hardened,  email: matt.joyce@protonmail.com }

    domains:
      - lostplot.com    # must be HIBP-verified via DNS TXT first
      - mattjoyce.ai    # must be HIBP-verified via DNS TXT first
```

## Pipeline wiring (suggested)

A pipeline that pipes deltas into `discord_notify`:

```yaml
pipelines:
  hibp_breach_alert:
    on_event: hibp.new_breach
    steps:
      - plugin: discord_notify
        command: handle
        params:
          channel: security-alerts
          template: |
            🚨 New breach hit on {{ payload.surface }} `{{ payload.label }}`
            Breach: {{ payload.breach_name }} ({{ payload.breach_date }})
            Data classes: {{ payload.data_classes | join(', ') }}
            ➡ {{ payload.recommended }}
```

## Event shape

```json
{
  "type": "hibp.new_breach",
  "payload": {
    "surface": "identity",
    "label": "gmail-primary",
    "breach_name": "Adobe",
    "breach_date": "2013-10-04",
    "data_classes": ["Email addresses", "Password hints", "Passwords"],
    "recommended": "Rotate credential for any service tied to gmail-primary; check if gmail-primary is recovery on Tier 0 accounts and detach."
  },
  "dedupe_key": "hibp:identity:<sha256>:Adobe"
}
```

## Privacy posture

- Plaintext email never written to state files or logs.
- State key is SHA-256 of lowercased email.
- Logs and Discord alerts surface the configured `label` (e.g. `gmail-primary`), not the address.
- API key lives in ductile's secrets config (`~/.config/ductile/secrets/...`), mode `0600`, gitignored.

## Health output

```
hibp_radar: paid_api=ready (6 identities, 2 domains)
```

or

```
hibp_radar: paid_api=DISABLED (no hibp_api_key configured)
```

## Operator handoff (what *you* need to do once)

1. **Get an HIBP API key** — sign up at <https://haveibeenpwned.com/API/Key>. Pwned 1 (~US$4/mo last public price; verify) covers `breachedaccount` and `breachedomain`. Stealerlogs is on a higher tier.
2. **Verify domain ownership** for `lostplot.com` and `mattjoyce.ai` — HIBP issues a TXT record via the API key dashboard; add it to DNS, click verify.
3. **Add config** to `~/.config/ductile/plugins.yaml` (or a dedicated `plugins/hibp_radar.yaml` include) using the sample above.
4. **Drop the API key** into the existing ductile secrets store; reference via the same mechanism `gmail_poller` uses for its tokens.
5. **Schedule poll** at 6h cadence — sufficient given HIBP's update tempo and your rate-limit window.
6. **Wire the pipeline** to `discord_notify` so deltas land in the same channel you already monitor.

## Tests

```bash
cd ~/Projects/ductile-plugins/hibp_radar
python3 -m unittest test_run -v
```
