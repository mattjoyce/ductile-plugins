#!/usr/bin/env python3
"""docling-pdf: gateway-side wrapper around the ductile-docling satellite.

Pipeline contract (parsem-needs-docling):

    on: parsem.needs_docling
    uses: docling-pdf
    with:
      source_path: "{payload.source_path}"
      doc_id:      "{payload.doc_id}"

The plugin POSTs {input_path, output_path} to <satellite_url>/convert and
blocks until the satellite returns 200 (ready) or non-2xx (failure). Per the
satellite contract: the .md is written LAST under an atomic-rename, so its
presence on disk is the completion signal for downstream filewatchers
(parsem_converted_watch picks it up and POSTs /ingest/converted-arrived).

Paths are *absolute, satellite-visible* paths. With the standard deploy
(parsem_library named volume bound to /mnt/user/Library/parsem-library and
mounted into the satellite at /library), callers pass /library/originals/...
for input and the plugin derives /library/inbound/converted/<doc_id>.md for
output unless `output_path` is given.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any

DEFAULT_TIMEOUT_SECONDS = 3600.0  # docling is CPU-bound; long-running OK
DEFAULT_OUTPUT_DIR = "/library/inbound/converted"


def ok_response(
    *,
    result: str,
    state_updates: dict[str, Any] | None = None,
    logs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    resp: dict[str, Any] = {"status": "ok", "result": result, "logs": logs or []}
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


class DoclingClient:
    """Thin urllib wrapper. Mockable in tests."""

    @staticmethod
    def post_convert(
        satellite_url: str,
        input_path: str,
        output_path: str,
        *,
        timeout: float,
    ) -> tuple[int, dict[str, Any] | None, str]:
        body = json.dumps({"input_path": input_path, "output_path": output_path}).encode()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "ductile/docling-pdf",
        }
        req = urllib.request.Request(
            f"{satellite_url}/convert", data=body, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                code = resp.getcode()
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            code = exc.code
        try:
            parsed: dict[str, Any] | None = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            parsed = None
        return code, parsed, raw

    @staticmethod
    def get_healthz(satellite_url: str, *, timeout: float = 5.0) -> tuple[int, str]:
        req = urllib.request.Request(
            f"{satellite_url}/healthz",
            headers={"User-Agent": "ductile/docling-pdf"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.getcode(), resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            return exc.code, ""


def _config(config: dict[str, Any]) -> tuple[str, str, float] | None:
    url = str(config.get("satellite_url") or "").rstrip("/")
    if not url:
        return None
    output_dir = str(config.get("output_dir") or DEFAULT_OUTPUT_DIR).rstrip("/")
    try:
        timeout = float(config.get("request_timeout") or DEFAULT_TIMEOUT_SECONDS)
    except (TypeError, ValueError):
        timeout = DEFAULT_TIMEOUT_SECONDS
    return url, output_dir, timeout


def _resolve_paths(
    payload: dict[str, Any], output_dir: str
) -> tuple[str, str, str] | str:
    """Returns (input_path, output_path, doc_id) or an error message string."""
    input_path = str(payload.get("source_path") or payload.get("input_path") or "").strip()
    if not input_path:
        return "missing required field: source_path"
    doc_id = str(payload.get("doc_id") or "").strip()
    output_path = str(payload.get("output_path") or "").strip()
    if not output_path:
        if not doc_id:
            doc_id = os.path.splitext(os.path.basename(input_path))[0]
        output_path = f"{output_dir}/{doc_id}.md"
    elif not doc_id:
        doc_id = os.path.splitext(os.path.basename(output_path))[0]
    for name, p in (("source_path", input_path), ("output_path", output_path)):
        if not p.startswith("/") or ".." in p.split("/"):
            return f"invalid {name}: {p!r} (must be absolute, no '..')"
    if not output_path.endswith(".md"):
        return f"output_path must end in .md (got {output_path!r})"
    return input_path, output_path, doc_id


def handle_handle(payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    cfg = _config(config)
    if cfg is None:
        return error_response("config.satellite_url is required", retry=False)
    satellite_url, output_dir, timeout = cfg

    resolved = _resolve_paths(payload, output_dir)
    if isinstance(resolved, str):
        return error_response(resolved, retry=False)
    input_path, output_path, doc_id = resolved

    try:
        code, parsed, raw = DoclingClient.post_convert(
            satellite_url, input_path, output_path, timeout=timeout
        )
    except (urllib.error.URLError, OSError) as exc:
        return error_response(f"docling satellite unreachable: {exc}", retry=True)

    if 200 <= code < 300 and isinstance(parsed, dict):
        return ok_response(
            result="ready",
            state_updates={
                "doc_id": doc_id,
                "state": "ready",
                "output_path": parsed.get("output_path", output_path),
                "page_count": parsed.get("page_count"),
                "parse_duration_seconds": parsed.get("parse_duration_seconds"),
                "docling_version": parsed.get("docling_version"),
            },
            logs=[
                {
                    "level": "info",
                    "message": (
                        f"docling converted doc_id={doc_id}: {input_path} -> "
                        f"{parsed.get('output_path', output_path)} "
                        f"({parsed.get('page_count','?')}p, "
                        f"{parsed.get('parse_duration_seconds','?')}s)"
                    ),
                }
            ],
        )

    # 400 = bad input (no point retrying), 503 = transient docling error.
    retry = code in (503,) or code == 0
    return error_response(
        f"docling /convert returned HTTP {code}: {raw[:300]}",
        retry=retry,
    )


def handle_health(config: dict[str, Any]) -> dict[str, Any]:
    cfg = _config(config)
    if cfg is None:
        return ok_response(
            result="degraded",
            logs=[{"level": "error", "message": "config.satellite_url is required"}],
        )
    satellite_url, _output_dir, _timeout = cfg
    try:
        code, body = DoclingClient.get_healthz(satellite_url)
    except (urllib.error.URLError, OSError) as exc:
        return ok_response(
            result="degraded",
            logs=[{"level": "error", "message": f"satellite unreachable: {exc}"}],
        )
    if 200 <= code < 300:
        return ok_response(
            result="healthy",
            logs=[{"level": "info", "message": f"satellite ok: {body[:200]}"}],
        )
    return ok_response(
        result="degraded",
        logs=[{"level": "warn", "message": f"satellite /healthz returned HTTP {code}"}],
    )


def handle_request(request: dict[str, Any]) -> dict[str, Any]:
    command = str(request.get("command") or "").strip()
    raw_config = request.get("config")
    config: dict[str, Any] = raw_config if isinstance(raw_config, dict) else {}

    payload: dict[str, Any] = {}
    raw_payload = request.get("payload")
    if isinstance(raw_payload, dict):
        payload.update(raw_payload)
    event = request.get("event")
    if isinstance(event, dict):
        event_payload = event.get("payload")
        if isinstance(event_payload, dict):
            payload.update(event_payload)
    for key in ("source_path", "input_path", "output_path", "doc_id"):
        if key not in payload and key in request:
            payload[key] = request[key]

    if command == "health":
        return handle_health(config)
    if command == "handle":
        return handle_handle(payload, config)
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
