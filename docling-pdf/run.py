#!/usr/bin/env python3
"""docling-pdf: PDF -> Markdown via IBM docling, then LLM polish.

Protocol v2 plugin. Two-stage, in-process pipeline:

  1. docling parses the source PDF into raw Markdown.
  2. a cloud LLM (Gemini by default, Claude optional) repairs the three
     classes of damage docling leaves behind: tables, heading structure,
     and footnotes/references. The polish prompt is a versioned artefact
     (polish_prompt_v1.txt) committed alongside this file.

Then it atomically writes <doc_id>.md and a <doc_id>.json sidecar to a
caller-specified output_dir. The sidecar lands first, the .md last, so a
filewatch on *.md is the definitive completion signal (same contract as
the marker and firecrawl plugins).

docling and the LLM SDKs are imported lazily inside the two seam
functions (`docling_convert`, `llm_polish`) so the test suite can mock
them without the heavy dependencies installed.

Config keys (all optional; polish is skippable for parse-only runs):
  llm_provider        - "gemini" (default) | "claude" | "none"
  gemini_api_key      - required when llm_provider == gemini
  gemini_model        - default: gemini-2.5-pro
  anthropic_api_key   - required when llm_provider == claude
  anthropic_model     - default: claude-sonnet-4-6
  polish_temperature  - default: 0.0
  llm_timeout_seconds - default: 120
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PLUGIN_VERSION = "0.1.0"
POLISH_PROMPT_FILE = "polish_prompt_v1.txt"
POLISH_PROMPT_VERSION = "v1"
DEFAULT_GEMINI_MODEL = "gemini-2.5-pro"
DEFAULT_ANTHROPIC_MODEL = "claude-sonnet-4-6"
DEFAULT_POLISH_TEMPERATURE = 0.0
DEFAULT_LLM_TIMEOUT_SECONDS = 120
# Generous output ceiling — a polished document can be as long as its
# input, and a truncated document is worse than an unpolished one.
ANTHROPIC_MAX_TOKENS = 64000


# ---------------------------------------------------------------------------
# Response helpers (envelope shared with firecrawl/marker plugins)
# ---------------------------------------------------------------------------


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


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PolishError(Exception):
    """Raised by llm_polish. `retry` tells the caller whether a retry
    could plausibly succeed (transport/5xx) or not (auth/4xx/bad-shape)."""

    def __init__(self, message: str, *, retry: bool) -> None:
        super().__init__(message)
        self.retry = retry


# ---------------------------------------------------------------------------
# Stage 1 — docling parse (lazy import so tests can mock this whole fn)
# ---------------------------------------------------------------------------


def docling_convert(source_path: str) -> tuple[str, str, int]:
    """Convert a PDF to Markdown with docling.

    Returns (markdown, docling_version, page_count). Raises RuntimeError
    on any docling failure — a corrupt/unsupported PDF will not convert
    on retry, so the caller treats this as non-retryable.
    """
    try:
        from importlib.metadata import version as _pkg_version

        from docling.document_converter import DocumentConverter
    except ImportError as exc:  # docling not installed
        raise RuntimeError(f"docling not importable: {exc}") from exc

    try:
        docling_version = _pkg_version("docling")
    except Exception:  # noqa: BLE001 - version is metadata only, never fatal
        docling_version = "unknown"

    try:
        converter = DocumentConverter()
        result = converter.convert(source_path)
        document = result.document
        markdown = document.export_to_markdown()
    except Exception as exc:  # noqa: BLE001 - docling raises a wide variety
        raise RuntimeError(f"docling conversion failed: {exc}") from exc

    page_count = 0
    try:
        pages = getattr(document, "pages", None)
        if pages is not None:
            page_count = len(pages)
    except Exception:  # noqa: BLE001 - page count is best-effort metadata
        page_count = 0

    return markdown, docling_version, page_count


# ---------------------------------------------------------------------------
# Stage 2 — LLM polish (lazy import so tests can mock this whole fn)
# ---------------------------------------------------------------------------


def _load_polish_prompt() -> str:
    """Read the versioned polish prompt sitting next to this file."""
    prompt_path = Path(__file__).with_name(POLISH_PROMPT_FILE)
    return prompt_path.read_text(encoding="utf-8")


def llm_polish(
    raw_markdown: str,
    *,
    provider: str,
    model: str,
    api_key: str,
    temperature: float,
    timeout_seconds: int,
) -> str:
    """Run the polish prompt over docling's raw markdown.

    Raises PolishError(retry=True) on transport/5xx, PolishError(
    retry=False) on auth/4xx/bad-shape. The prompt is loaded from the
    versioned file and the raw markdown substituted into it.
    """
    prompt = _load_polish_prompt().replace("{raw_markdown}", raw_markdown)

    if provider == "gemini":
        return _polish_gemini(
            prompt, model=model, api_key=api_key,
            temperature=temperature, timeout_seconds=timeout_seconds,
        )
    if provider == "claude":
        return _polish_claude(
            prompt, model=model, api_key=api_key,
            temperature=temperature, timeout_seconds=timeout_seconds,
        )
    raise PolishError(f"unknown llm_provider: {provider!r}", retry=False)


def _polish_gemini(
    prompt: str,
    *,
    model: str,
    api_key: str,
    temperature: float,
    timeout_seconds: int,
) -> str:
    try:
        from google import genai
        from google.genai import types as genai_types
    except ImportError as exc:
        raise PolishError(f"google-genai not importable: {exc}", retry=False) from exc

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                temperature=temperature,
                http_options=genai_types.HttpOptions(
                    timeout=timeout_seconds * 1000
                ),
            ),
        )
    except Exception as exc:  # noqa: BLE001 - genai raises provider-specific
        raise PolishError(
            f"gemini call failed: {exc}", retry=_looks_transient(exc)
        ) from exc

    text = getattr(response, "text", None)
    if not isinstance(text, str) or not text.strip():
        raise PolishError("gemini returned empty response", retry=False)
    return text


def _polish_claude(
    prompt: str,
    *,
    model: str,
    api_key: str,
    temperature: float,
    timeout_seconds: int,
) -> str:
    try:
        import anthropic
    except ImportError as exc:
        raise PolishError(f"anthropic not importable: {exc}", retry=False) from exc

    try:
        client = anthropic.Anthropic(api_key=api_key, timeout=float(timeout_seconds))
        message = client.messages.create(
            model=model,
            max_tokens=ANTHROPIC_MAX_TOKENS,
            temperature=temperature,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:  # noqa: BLE001 - anthropic raises provider-specific
        raise PolishError(
            f"claude call failed: {exc}", retry=_looks_transient(exc)
        ) from exc

    parts = [
        block.text
        for block in getattr(message, "content", [])
        if getattr(block, "type", None) == "text"
    ]
    text = "".join(parts)
    if not text.strip():
        raise PolishError("claude returned empty response", retry=False)
    return text


def _looks_transient(exc: Exception) -> bool:
    """Heuristic: 5xx / timeout / connection errors are worth retrying;
    auth and 4xx are not. We avoid importing provider exception types, so
    sniff the stringified error and any `status_code` attribute."""
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status >= 500 or status == 429
    blob = f"{type(exc).__name__}: {exc}".lower()
    transient_markers = (
        "timeout", "timed out", "connection", "temporarily",
        "503", "502", "500", "overloaded", "unavailable",
    )
    return any(m in blob for m in transient_markers)


# ---------------------------------------------------------------------------
# Output contract
# ---------------------------------------------------------------------------


def atomic_write_outputs(
    *,
    output_dir: Path,
    doc_id: str,
    markdown: str,
    sidecar: dict[str, Any],
) -> Path:
    """Write <doc_id>.md and <doc_id>.json atomically.

    Sidecar lands first; .md lands last so a filewatch on *.md fires only
    when both are in place. os.replace is POSIX-atomic on one filesystem.
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


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def _resolve_provider(config: dict[str, Any]) -> tuple[str, str, str]:
    """Return (provider, model, api_key) from config. provider is one of
    gemini|claude|none. api_key is "" for the none provider."""
    provider = str(config.get("llm_provider") or "gemini").strip().lower()
    if provider == "none":
        return "none", "", ""
    if provider == "claude":
        model = str(config.get("anthropic_model") or DEFAULT_ANTHROPIC_MODEL).strip()
        api_key = str(config.get("anthropic_api_key") or "").strip()
        return "claude", model, api_key
    # default + explicit gemini
    model = str(config.get("gemini_model") or DEFAULT_GEMINI_MODEL).strip()
    api_key = str(config.get("gemini_api_key") or "").strip()
    return "gemini", model, api_key


def handle_health(config: dict[str, Any]) -> dict[str, Any]:
    logs: list[dict[str, str]] = []

    try:
        import docling  # noqa: F401
        docling_ok = True
    except ImportError as exc:
        docling_ok = False
        logs.append({"level": "error", "message": f"docling not importable: {exc}"})

    provider, _model, api_key = _resolve_provider(config)
    if provider != "none" and not api_key:
        return error_response(
            f"llm_provider={provider} but its API key is not configured",
            retry=False,
            logs=logs + [{"level": "error", "message": f"missing API key for {provider}"}],
        )
    if not docling_ok:
        return error_response(
            "docling not importable",
            retry=False,
            logs=logs,
        )
    return ok_response(
        result="healthy",
        logs=[{"level": "info", "message": f"docling-pdf healthy (provider={provider})"}],
    )


def handle_handle(config: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    source = str(payload.get("source") or "").strip()
    doc_id = str(payload.get("doc_id") or "").strip()
    output_dir_str = str(payload.get("output_dir") or "").strip()

    if not source:
        return error_response("payload.source required", retry=False)
    if not doc_id:
        return error_response("payload.doc_id required", retry=False)
    if not output_dir_str:
        return error_response("payload.output_dir required", retry=False)

    # doc_id is a filename component — refuse separators / traversal.
    # This is a public-repo plugin; the caller is untrusted from here.
    if "/" in doc_id or "\\" in doc_id or doc_id.startswith(".") or ".." in doc_id:
        return error_response(
            f"payload.doc_id contains illegal characters: {doc_id!r}",
            retry=False,
        )

    source_path = Path(source)
    if not source_path.is_file():
        return error_response(
            f"source does not exist or is not a file: {source}",
            retry=False,
        )

    output_dir = Path(output_dir_str)
    if not output_dir.is_dir():
        return error_response(
            f"output_dir does not exist or is not a directory: {output_dir_str}",
            retry=False,
        )
    # No os.access W_OK pre-check: it's racy and false-negatives on
    # NAS/ACL filesystems (this plugin writes to a NAS share). The
    # atomic write is already wrapped in `except OSError` below.

    provider, model, api_key = _resolve_provider(config)
    if provider != "none" and not api_key:
        return error_response(
            f"llm_provider={provider} but its API key is not configured",
            retry=False,
        )
    temperature = float(config.get("polish_temperature") or DEFAULT_POLISH_TEMPERATURE)
    timeout_seconds = int(config.get("llm_timeout_seconds") or DEFAULT_LLM_TIMEOUT_SECONDS)

    started_iso = now_iso()

    # --- Stage 1: docling parse ---
    parse_start = time.monotonic()
    try:
        raw_markdown, docling_version, page_count = docling_convert(source)
    except RuntimeError as exc:
        return error_response(
            f"docling parse failed: {exc}",
            retry=False,
            logs=[{"level": "error", "message": f"docling parse failed for {source}: {exc}"}],
        )
    parse_duration = round(time.monotonic() - parse_start, 3)

    # --- Stage 2: LLM polish (skippable) ---
    polish_skipped_reason: str | None = None
    llm_model: str | None = None
    polish_duration = 0.0
    if provider == "none":
        polished_markdown = raw_markdown
        polish_skipped_reason = "llm_provider=none"
    else:
        polish_start = time.monotonic()
        try:
            polished_markdown = llm_polish(
                raw_markdown,
                provider=provider,
                model=model,
                api_key=api_key,
                temperature=temperature,
                timeout_seconds=timeout_seconds,
            )
        except PolishError as exc:
            return error_response(
                f"llm polish failed: {exc}",
                retry=exc.retry,
                logs=[{"level": "error", "message": f"polish failed ({provider}): {exc}"}],
            )
        polish_duration = round(time.monotonic() - polish_start, 3)
        llm_model = model

    sidecar: dict[str, Any] = {
        "doc_id": doc_id,
        "status": "ready",
        "docling_pdf_version": PLUGIN_VERSION,
        "docling_version": docling_version,
        "llm_provider": provider,
        "llm_model": llm_model,
        "polish_prompt_version": POLISH_PROMPT_VERSION,
        "page_count": page_count,
        "parse_duration_seconds": parse_duration,
        "polish_duration_seconds": polish_duration,
        "polish_skipped_reason": polish_skipped_reason,
        "source": source,
        "started_at": started_iso,
        "completed_at": now_iso(),
    }

    try:
        md_path = atomic_write_outputs(
            output_dir=output_dir,
            doc_id=doc_id,
            markdown=polished_markdown,
            sidecar=sidecar,
        )
    except OSError as exc:
        return error_response(
            f"failed to write outputs: {exc}",
            retry=True,
            logs=[{"level": "error", "message": f"atomic write failed: {exc}"}],
        )

    return ok_response(
        result=(
            f"converted {source} -> {md_path} "
            f"({page_count}p, parse {parse_duration}s, polish {polish_duration}s)"
        ),
        events=[
            {
                "type": "content_ready",
                "payload": {
                    "doc_id": doc_id,
                    "output_path": str(md_path),
                    "page_count": page_count,
                    "parse_duration_seconds": parse_duration,
                    "polish_duration_seconds": polish_duration,
                },
            }
        ],
        logs=[
            {
                "level": "info",
                "message": (
                    f"docling-pdf converted doc_id={doc_id}: {page_count} pages, "
                    f"parse {parse_duration}s, polish {polish_duration}s "
                    f"(provider={provider}, model={llm_model})"
                ),
            }
        ],
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def handle_request(request: dict[str, Any]) -> dict[str, Any]:
    command = str(request.get("command") or "").strip()
    config = request.get("config")
    if not isinstance(config, dict):
        config = {}

    # Assemble payload from every place ductile might put it: a pipeline
    # step's `with:` remap lands under event.payload; direct API callers
    # use top-level payload; some callers pass flat fields. Later wins.
    event: dict[str, Any] = {}
    raw_event = request.get("event")
    if isinstance(raw_event, dict):
        event = raw_event
    merged_payload: dict[str, Any] = {}
    raw_payload = request.get("payload")
    if isinstance(raw_payload, dict):
        merged_payload.update(raw_payload)
    event_payload = event.get("payload")
    if isinstance(event_payload, dict):
        merged_payload.update(event_payload)
    for key in ("source", "doc_id", "output_dir"):
        if key not in merged_payload and key in request:
            merged_payload[key] = request[key]
    event = {**event, "payload": merged_payload}

    if command == "health":
        return handle_health(config)
    if command == "handle":
        return handle_handle(config, event)
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
