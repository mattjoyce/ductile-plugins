#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""hibp_radar — Ductile plugin (protocol v2).

Monitors configured email identities and owned domains for new appearances in
HIBP (Have I Been Pwned) breach dumps. Emits one hibp.new_breach event per
breach name that has not been seen before for that identity or domain.

Privacy posture:
  - Plaintext email never persisted to state or logs. State keys identities by
    SHA-256 of the lowercased address. Logs emit the configured `label` only.
  - Pwned Passwords (k-anonymity) is NOT used here — this plugin queries email
    surfaces only.

Endpoints used:
  - GET https://haveibeenpwned.com/api/v3/breachedaccount/{email}
        Requires hibp-api-key header. Lowest paid HIBP tier (Pwned 1) covers it.
  - GET https://haveibeenpwned.com/api/v3/breachedomain/{domain}
        Requires hibp-api-key header. Free for domains the API key holder has
        verified ownership of via DNS TXT record.

Stealerlogs endpoint is deferred (requires higher paid tier).

Config keys (all optional):
  hibp_api_key      (str)   — HIBP v3 API key. Without it, identity queries are skipped.
  user_agent_email  (str)   — Contact email embedded in User-Agent (HIBP requirement).
  identities        (list)  — [{label: str, email: str}, ...]
  domains           (list)  — [str, ...]  (must be HIBP-verified by the API key holder)
  rate_limit_ms     (int)   — Sleep between API calls (default 1600). HIBP requires ≥1500ms on lower tiers.
  timeout_seconds   (int)   — HTTP timeout per request (default 20).

State shape (persisted via state_updates):
  identities:
    "<sha256_of_lowercased_email>":
      label: str
      breaches_seen: list[str]
      last_polled_at: str (ISO-8601)
  domains:
    "<domain>":
      breaches_seen: list[str]    # breach names known to involve this domain
      last_polled_at: str

Event emitted: hibp.new_breach
  payload.surface     — "identity" | "domain"
  payload.label       — identity label or domain name (NEVER full email)
  payload.breach_name — HIBP breach Name field
  payload.breach_date — Breach BreachDate (YYYY-MM-DD) when present
  payload.data_classes— list[str] (e.g., ["Email addresses", "Passwords"])
  payload.recommended — short action hint string
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

HIBP_BASE = "https://haveibeenpwned.com/api/v3"
DEFAULT_RATE_LIMIT_MS = 1600
DEFAULT_TIMEOUT_S = 20


# ── response helpers ──────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ok(
    result: str,
    logs: list[dict] | None = None,
    events: list[dict] | None = None,
    state_updates: dict | None = None,
) -> dict[str, Any]:
    resp: dict[str, Any] = {
        "status": "ok",
        "result": result,
        "logs": logs or [{"level": "info", "message": result}],
    }
    if events:
        resp["events"] = events
    if state_updates is not None:
        resp["state_updates"] = state_updates
    return resp


def err(message: str, *, retry: bool = False, logs: list[dict] | None = None) -> dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": logs or [{"level": "error", "message": message}],
    }


# ── HIBP API wrapper ──────────────────────────────────────────────────────────

class HIBPError(Exception):
    def __init__(self, message: str, *, status: int = 0, retry: bool = False):
        super().__init__(message)
        self.status = status
        self.retry = retry


def hibp_get(
    path: str,
    *,
    api_key: str,
    user_agent_email: str,
    timeout: int,
    params: dict[str, str] | None = None,
) -> Any:
    """GET an HIBP v3 endpoint and return parsed JSON, or [] on 404 (no breaches)."""
    url = f"{HIBP_BASE}{path}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"

    contact = user_agent_email or "ductile-hibp-radar"
    headers = {
        "User-Agent": f"ductile-hibp-radar/0.1 ({contact})",
        "hibp-api-key": api_key,
        "Accept": "application/json",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            if not body:
                return []
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return []  # HIBP convention: 404 means no breaches found
        if exc.code in (401, 403):
            raise HIBPError(f"auth/permission ({exc.code}): {exc.reason}", status=exc.code, retry=False)
        if exc.code == 429:
            raise HIBPError("rate limited (429)", status=429, retry=True)
        if 500 <= exc.code < 600:
            raise HIBPError(f"server error ({exc.code})", status=exc.code, retry=True)
        raise HIBPError(f"http {exc.code}: {exc.reason}", status=exc.code, retry=False)
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        raise HIBPError(f"network error: {exc}", retry=True)


# ── identity hashing ──────────────────────────────────────────────────────────

def id_hash(email: str) -> str:
    return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()


# ── recommendations (small, deterministic; no LLM) ────────────────────────────

def recommend_for_identity(label: str, data_classes: list[str]) -> str:
    has_passwords = any("password" in dc.lower() for dc in data_classes)
    if has_passwords:
        return f"Rotate credential for any service tied to {label}; check if {label} is recovery on Tier 0 accounts and detach."
    return f"Verify which service breach this is and audit current usage of {label} there."


def recommend_for_domain(domain: str) -> str:
    return f"Audit which addresses on {domain} are affected; rotate where credentials are tied to those addresses."


# ── health ────────────────────────────────────────────────────────────────────

def cmd_health(config: dict) -> dict:
    api_key = str(config.get("hibp_api_key") or "").strip()
    identities = config.get("identities") or []
    domains = config.get("domains") or []

    surfaces: list[str] = []
    if api_key:
        surfaces.append(f"paid_api=ready ({len(identities)} identities, {len(domains)} domains)")
    else:
        surfaces.append("paid_api=DISABLED (no hibp_api_key configured)")

    if not identities and not domains:
        surfaces.append("no surfaces configured — add identities and/or domains")

    msg = "hibp_radar: " + " | ".join(surfaces)
    return ok(msg, logs=[{"level": "info", "message": msg}])


# ── poll ──────────────────────────────────────────────────────────────────────

def merge_identity_state(
    old: dict,
    *,
    h: str,
    label: str,
    new_breaches: list[str],
    polled_at: str,
) -> tuple[dict, list[str]]:
    """Return (updated_identity_dict, list_of_new_breach_names)."""
    prior = old.get(h) or {}
    seen: list[str] = list(prior.get("breaches_seen") or [])
    delta = [b for b in new_breaches if b not in seen]
    merged_seen = seen + delta
    return {
        "label": label,
        "breaches_seen": merged_seen,
        "last_polled_at": polled_at,
    }, delta


def merge_domain_state(
    old: dict,
    *,
    domain: str,
    new_breach_keys: list[str],
    polled_at: str,
) -> tuple[dict, list[str]]:
    """For domains, HIBP returns a map of {alias: [breach_names]}.

    We track unique <alias|breach> tuples as breach keys to detect deltas.
    Returns (updated_domain_dict, list_of_new_breach_keys).
    """
    prior = old.get(domain) or {}
    seen: list[str] = list(prior.get("breaches_seen") or [])
    delta = [k for k in new_breach_keys if k not in seen]
    merged_seen = seen + delta
    return {
        "breaches_seen": merged_seen,
        "last_polled_at": polled_at,
    }, delta


def poll_identities(
    identities: list[dict],
    *,
    api_key: str,
    user_agent_email: str,
    rate_limit_ms: int,
    timeout: int,
    prior_state: dict,
) -> tuple[dict, list[dict], list[dict]]:
    """Returns (updated_state_section, events, logs)."""
    updated: dict[str, Any] = dict(prior_state)
    events: list[dict] = []
    logs: list[dict] = []
    polled_at = now_iso()

    for idx, item in enumerate(identities):
        if not isinstance(item, dict):
            logs.append({"level": "warn", "message": f"identities[{idx}] not a dict — skipping"})
            continue
        email = str(item.get("email") or "").strip()
        label = str(item.get("label") or "").strip() or f"identity-{idx}"
        if not email:
            logs.append({"level": "warn", "message": f"identities[{idx}] missing email — skipping"})
            continue
        h = id_hash(email)

        try:
            breaches = hibp_get(
                f"/breachedaccount/{urllib.parse.quote(email)}",
                api_key=api_key,
                user_agent_email=user_agent_email,
                timeout=timeout,
                params={"truncateResponse": "false"},
            )
        except HIBPError as exc:
            logs.append({"level": "warn", "message": f"breachedaccount[{label}] failed: {exc}"})
            continue

        breach_objs = breaches if isinstance(breaches, list) else []
        breach_names = [str(b.get("Name") or "") for b in breach_objs if isinstance(b, dict) and b.get("Name")]

        new_section, delta = merge_identity_state(
            prior_state, h=h, label=label, new_breaches=breach_names, polled_at=polled_at
        )
        updated[h] = new_section

        # Build per-delta events using the matching breach object for metadata
        breach_by_name = {str(b.get("Name") or ""): b for b in breach_objs if isinstance(b, dict)}
        for name in delta:
            obj = breach_by_name.get(name) or {}
            data_classes = [str(c) for c in (obj.get("DataClasses") or [])]
            events.append({
                "type": "hibp.new_breach",
                "payload": {
                    "surface": "identity",
                    "label": label,
                    "breach_name": name,
                    "breach_date": str(obj.get("BreachDate") or ""),
                    "data_classes": data_classes,
                    "recommended": recommend_for_identity(label, data_classes),
                },
                "dedupe_key": f"hibp:identity:{h}:{name}",
            })

        logs.append({
            "level": "info",
            "message": f"polled identity[{label}]: {len(breach_names)} known, {len(delta)} new",
        })

        if idx < len(identities) - 1:
            time.sleep(rate_limit_ms / 1000.0)

    return updated, events, logs


def poll_domains(
    domains: list[str],
    *,
    api_key: str,
    user_agent_email: str,
    rate_limit_ms: int,
    timeout: int,
    prior_state: dict,
) -> tuple[dict, list[dict], list[dict]]:
    updated: dict[str, Any] = dict(prior_state)
    events: list[dict] = []
    logs: list[dict] = []
    polled_at = now_iso()

    for idx, domain in enumerate(domains):
        domain = str(domain or "").strip().lower()
        if not domain:
            continue

        try:
            data = hibp_get(
                f"/breachedomain/{urllib.parse.quote(domain)}",
                api_key=api_key,
                user_agent_email=user_agent_email,
                timeout=timeout,
            )
        except HIBPError as exc:
            logs.append({"level": "warn", "message": f"breachedomain[{domain}] failed: {exc}"})
            continue

        # HIBP returns {alias: [breach_names]} for breached domains
        if not isinstance(data, dict):
            data = {}

        # Compose breach keys as "alias|breach_name" — alias is local-part only,
        # not full email, but still on disk. Hash alias to keep state opaque.
        new_keys: list[str] = []
        for alias, names in data.items():
            alias_hash = hashlib.sha256(str(alias).strip().lower().encode("utf-8")).hexdigest()[:16]
            if isinstance(names, list):
                for n in names:
                    if n:
                        new_keys.append(f"{alias_hash}|{n}")

        new_section, delta = merge_domain_state(
            prior_state, domain=domain, new_breach_keys=new_keys, polled_at=polled_at
        )
        updated[domain] = new_section

        for key in delta:
            _, breach_name = key.split("|", 1)
            events.append({
                "type": "hibp.new_breach",
                "payload": {
                    "surface": "domain",
                    "label": domain,
                    "breach_name": breach_name,
                    "breach_date": "",
                    "data_classes": [],
                    "recommended": recommend_for_domain(domain),
                },
                "dedupe_key": f"hibp:domain:{domain}:{key}",
            })

        logs.append({
            "level": "info",
            "message": f"polled domain[{domain}]: {len(new_keys)} known keys, {len(delta)} new",
        })

        if idx < len(domains) - 1:
            time.sleep(rate_limit_ms / 1000.0)

    return updated, events, logs


def cmd_poll(config: dict, state: dict) -> dict:
    api_key = str(config.get("hibp_api_key") or "").strip()
    user_agent_email = str(config.get("user_agent_email") or "").strip()
    identities = config.get("identities") or []
    domains = config.get("domains") or []
    rate_limit_ms = int(config.get("rate_limit_ms") or DEFAULT_RATE_LIMIT_MS)
    timeout = int(config.get("timeout_seconds") or DEFAULT_TIMEOUT_S)

    prior_identities = state.get("identities") or {}
    prior_domains = state.get("domains") or {}

    all_events: list[dict] = []
    all_logs: list[dict] = []

    if not api_key:
        all_logs.append({
            "level": "warn",
            "message": "no hibp_api_key configured — all surfaces skipped this poll",
        })
        # No state changes; return early but preserve prior state.
        return ok(
            "hibp_radar poll: skipped (no api key)",
            logs=all_logs,
            state_updates={"identities": prior_identities, "domains": prior_domains},
        )

    new_identities = prior_identities
    new_domains = prior_domains

    if identities:
        new_identities, ev, lg = poll_identities(
            identities,
            api_key=api_key,
            user_agent_email=user_agent_email,
            rate_limit_ms=rate_limit_ms,
            timeout=timeout,
            prior_state=prior_identities,
        )
        all_events.extend(ev)
        all_logs.extend(lg)

    if domains:
        new_domains, ev, lg = poll_domains(
            domains,
            api_key=api_key,
            user_agent_email=user_agent_email,
            rate_limit_ms=rate_limit_ms,
            timeout=timeout,
            prior_state=prior_domains,
        )
        all_events.extend(ev)
        all_logs.extend(lg)

    summary = (
        f"hibp_radar poll: {len(all_events)} new breach event(s) "
        f"({len(identities)} identities, {len(domains)} domains)"
    )
    all_logs.append({"level": "info", "message": summary})

    return ok(
        summary,
        logs=all_logs,
        events=all_events,
        state_updates={"identities": new_identities, "domains": new_domains},
    )


# ── handle (HIBP domain push webhook) ─────────────────────────────────────────

def cmd_handle(config: dict, event: dict, state: dict) -> dict:
    """HIBP domain-subscription pushes payloads of the form documented at
    https://haveibeenpwned.com/API/v3#NotifyMeOfBreachesOnMyDomain.

    We accept a ductile event with payload mirroring HIBP's POST body:
      payload.domain    — affected domain (must be in configured domains list)
      payload.breach    — { Name, BreachDate, DataClasses, ... } (HIBP breach object)

    Emits a hibp.new_breach event if not already in state.
    """
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    domain = str(payload.get("domain") or "").strip().lower()
    breach = payload.get("breach") if isinstance(payload.get("breach"), dict) else {}
    breach_name = str(breach.get("Name") or "").strip()

    if not domain or not breach_name:
        return err("handle: payload requires domain and breach.Name")

    configured_domains = [str(d or "").strip().lower() for d in (config.get("domains") or [])]
    if domain not in configured_domains:
        return err(f"handle: domain {domain!r} not in configured domains")

    prior_domains: dict[str, Any] = dict(state.get("domains") or {})
    prior = prior_domains.get(domain) or {"breaches_seen": [], "last_polled_at": ""}
    seen = list(prior.get("breaches_seen") or [])

    # Use a stable key (breach_name only here — push events don't enumerate aliases).
    key = f"push|{breach_name}"
    if key in seen:
        return ok(
            f"hibp_radar handle: {breach_name} already seen for {domain}",
            state_updates={"identities": state.get("identities") or {}, "domains": prior_domains},
        )

    new_seen = seen + [key]
    prior_domains[domain] = {"breaches_seen": new_seen, "last_polled_at": now_iso()}

    data_classes = [str(c) for c in (breach.get("DataClasses") or [])]
    event_out = {
        "type": "hibp.new_breach",
        "payload": {
            "surface": "domain",
            "label": domain,
            "breach_name": breach_name,
            "breach_date": str(breach.get("BreachDate") or ""),
            "data_classes": data_classes,
            "recommended": recommend_for_domain(domain),
        },
        "dedupe_key": f"hibp:domain-push:{domain}:{breach_name}",
    }

    return ok(
        f"hibp_radar handle: emitted {breach_name} for {domain}",
        events=[event_out],
        state_updates={"identities": state.get("identities") or {}, "domains": prior_domains},
    )


# ── entrypoint ────────────────────────────────────────────────────────────────

def handle_request(req: dict) -> dict:
    command = str(req.get("command") or "").strip()
    config = req.get("config") if isinstance(req.get("config"), dict) else {}
    state = req.get("state") if isinstance(req.get("state"), dict) else {}
    event = req.get("event") if isinstance(req.get("event"), dict) else {}

    if command == "health":
        return cmd_health(config)
    if command == "poll":
        return cmd_poll(config, state)
    if command == "handle":
        return cmd_handle(config, event, state)
    return err(f"unknown command: {command!r}")


def main() -> None:
    try:
        req = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        json.dump(err(f"invalid JSON input: {exc}"), sys.stdout)
        sys.stdout.write("\n")
        sys.exit(1)

    out = handle_request(req)
    json.dump(out, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
