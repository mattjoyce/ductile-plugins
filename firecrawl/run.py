#!/usr/bin/env python3
"""firecrawl: Scrape URLs via the Firecrawl API (api.firecrawl.dev).

Protocol v2 plugin. Calls Firecrawl's REST /scrape endpoint to convert
URLs to clean markdown, then atomically writes <doc_id>.md and
<doc_id>.json sidecar to a caller-specified output directory.

Companion to filesystem-based ingest pipelines: the caller (e.g. an
ingest service) passes output_dir; this plugin writes <doc_id>.md
atomically; a filewatcher on output_dir is the completion signal.

Config keys:
  firecrawl_api_key  - Firecrawl API key (required)
  api_base_url       - Override Firecrawl base URL (default: api.firecrawl.dev/v1)
  timeout_seconds    - Per-request timeout in seconds (default: 60)
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_API_BASE_URL = "https://api.firecrawl.dev/v1"
DEFAULT_TIMEOUT_SECONDS = 60
PLUGIN_VERSION = "0.1.0"


def ok_response(
    *,
    result: str,
    events: list[dict[str, Any]] | None = None,
    state_updates: dict[str, Any] | None = None,
    logs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    resp: dict[str, Any] = {
        "status": "ok",
        "result": result,
        "logs": logs or [],
    }
    if events:
        resp["events"] = events
    if state_updates:
        resp["state_updates"] = state_updates
    return resp


def error_response(
    message: str,
    *,
    retry: bool = True,
    logs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": logs or [{"level": "error", "message": message}],
    }


def content_hash(text: str) -> str:
    """SHA-256 prefix hash of content (16 hex chars)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def call_firecrawl(
    url: str,
    *,
    api_key: str,
    api_base_url: str,
    timeout_seconds: int,
) -> tuple[str, dict[str, Any]]:
    """POST to Firecrawl's /scrape endpoint, return (markdown, metadata).

    Raises urllib.error.URLError / HTTPError / OSError on transport
    failure. Raises ValueError if the response shape is unexpected.
    """
    payload = json.dumps({"url": url, "formats": ["markdown"]}).encode("utf-8")
    req = urllib.request.Request(
        f"{api_base_url.rstrip('/')}/scrape",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": f"ductile/firecrawl/{PLUGIN_VERSION}",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        body = resp.read()
        status_code = resp.getcode()

    parsed = json.loads(body.decode("utf-8"))
    if not isinstance(parsed, dict) or not parsed.get("success"):
        raise ValueError(f"firecrawl returned non-success: {parsed!r}")
    data = parsed.get("data") or {}
    markdown = data.get("markdown")
    if not isinstance(markdown, str):
        raise ValueError("firecrawl response missing data.markdown")

    metadata = data.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    metadata["_status_code"] = status_code
    return markdown, metadata


def atomic_write_outputs(
    *,
    output_dir: Path,
    doc_id: str,
    markdown: str,
    sidecar: dict[str, Any],
) -> Path:
    """Write <doc_id>.md and <doc_id>.json atomically.

    Sidecar lands first; .md lands last so a filewatch on *.md fires
    only when both are in place. Uses os.replace for POSIX-atomic rename
    on the same filesystem.
    """
    md_final = output_dir / f"{doc_id}.md"
    json_final = output_dir / f"{doc_id}.json"
    md_tmp = output_dir / f".{doc_id}.md.tmp"
    json_tmp = output_dir / f".{doc_id}.json.tmp"

    json_tmp.write_text(json.dumps(sidecar, indent=2, sort_keys=True), encoding="utf-8")
    md_tmp.write_text(markdown, encoding="utf-8")
    os.replace(json_tmp, json_final)
    os.replace(md_tmp, md_final)
    return md_final


def handle_health(config: dict[str, Any]) -> dict[str, Any]:
    api_key = str(config.get("firecrawl_api_key") or "").strip()
    if not api_key:
        return error_response(
            "firecrawl_api_key not configured",
            retry=False,
            logs=[{"level": "error", "message": "missing firecrawl_api_key in plugin config"}],
        )
    return ok_response(
        result="healthy",
        logs=[{"level": "info", "message": "firecrawl plugin healthy"}],
    )


def handle_scrape(config: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    api_key = str(config.get("firecrawl_api_key") or "").strip()
    if not api_key:
        return error_response(
            "firecrawl_api_key not configured",
            retry=False,
            logs=[{"level": "error", "message": "missing firecrawl_api_key in plugin config"}],
        )

    api_base_url = str(config.get("api_base_url") or DEFAULT_API_BASE_URL).strip()
    timeout_seconds = int(config.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)

    payload = event.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    url = str(payload.get("url") or "").strip()
    doc_id = str(payload.get("doc_id") or "").strip()
    output_dir_str = str(payload.get("output_dir") or "").strip()

    if not url:
        return error_response("payload.url required", retry=False)
    if not doc_id:
        return error_response("payload.doc_id required", retry=False)
    if not output_dir_str:
        return error_response("payload.output_dir required", retry=False)

    # doc_id must be a safe filename component — refuse any path separator
    # or traversal char. This is a public-repo plugin and the caller is
    # untrusted from the plugin's perspective.
    if "/" in doc_id or "\\" in doc_id or doc_id.startswith(".") or ".." in doc_id:
        return error_response(
            f"payload.doc_id contains illegal characters: {doc_id!r}",
            retry=False,
        )

    output_dir = Path(output_dir_str)
    if not output_dir.is_dir():
        return error_response(
            f"output_dir does not exist or is not a directory: {output_dir_str}",
            retry=False,
        )
    if not os.access(output_dir, os.W_OK):
        return error_response(
            f"output_dir not writable: {output_dir_str}",
            retry=False,
        )

    started = time.monotonic()
    started_iso = now_iso()
    try:
        markdown, metadata = call_firecrawl(
            url,
            api_key=api_key,
            api_base_url=api_base_url,
            timeout_seconds=timeout_seconds,
        )
    except urllib.error.HTTPError as exc:
        # 4xx generally not retryable (bad URL, auth); 5xx retryable.
        retry = exc.code >= 500
        return error_response(
            f"firecrawl HTTP {exc.code}: {exc.reason}",
            retry=retry,
            logs=[{"level": "error", "message": f"firecrawl HTTPError {exc.code} for {url}"}],
        )
    except (urllib.error.URLError, OSError) as exc:
        return error_response(
            f"firecrawl transport error: {exc}",
            retry=True,
            logs=[{"level": "error", "message": f"firecrawl transport error for {url}: {exc}"}],
        )
    except (ValueError, json.JSONDecodeError) as exc:
        return error_response(
            f"firecrawl response invalid: {exc}",
            retry=False,
            logs=[{"level": "error", "message": f"firecrawl response invalid: {exc}"}],
        )

    duration_seconds = round(time.monotonic() - started, 3)
    hash_hex = content_hash(markdown)
    final_url = str(metadata.get("sourceURL") or metadata.get("url") or url)

    sidecar = {
        "doc_id":            doc_id,
        "status":            "ready",
        "url":               url,
        "final_url":         final_url,
        "firecrawl_version": PLUGIN_VERSION,
        "duration_seconds":  duration_seconds,
        "status_code":       int(metadata.get("_status_code", 200)),
        "started_at":        started_iso,
        "completed_at":      now_iso(),
        "content_hash":      hash_hex,
        "title":             metadata.get("title"),
        "description":       metadata.get("description"),
    }

    try:
        md_path = atomic_write_outputs(
            output_dir=output_dir,
            doc_id=doc_id,
            markdown=markdown,
            sidecar=sidecar,
        )
    except OSError as exc:
        return error_response(
            f"failed to write outputs: {exc}",
            retry=True,
            logs=[{"level": "error", "message": f"atomic write failed: {exc}"}],
        )

    return ok_response(
        result=f"scraped {url} ({len(markdown)} bytes, {duration_seconds}s)",
        events=[
            {
                "type": "content_ready",
                "payload": {
                    "url":              url,
                    "doc_id":           doc_id,
                    "output_path":      str(md_path),
                    "content_hash":     hash_hex,
                    "duration_seconds": duration_seconds,
                },
            }
        ],
        logs=[
            {"level": "info", "message": f"firecrawl scraped {url} → {md_path} ({duration_seconds}s)"},
        ],
    )


def handle_request(request: dict[str, Any]) -> dict[str, Any]:
    command = str(request.get("command") or "").strip()
    config = request.get("config")
    if not isinstance(config, dict):
        config = {}
    event = request.get("event")
    if not isinstance(event, dict):
        event = {}

    if command == "health":
        return handle_health(config)
    if command == "handle":
        return handle_scrape(config, event)
    return error_response(
        f"unknown command: {command}",
        retry=False,
        logs=[{"level": "error", "message": f"unknown command: {command}"}],
    )


def main() -> None:
    request = json.load(sys.stdin)
    response = handle_request(request)
    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
