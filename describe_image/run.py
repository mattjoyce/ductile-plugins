#!/usr/bin/env python3
"""describe_image: write a Markdown sidecar describing an image, via the Claude API.

Trusted-tier (credentialed) plugin. It runs as the operator so it can read the
watched folder inside the operator's home and write the sidecar next to the
image. Stdlib only: the confined/credentialed runtime contract forbids fetching
dependencies at spawn and the anthropic SDK is not installed on the host, so
the Messages API is called over raw HTTP with urllib.

Input event (from folder_watch, emit_mode=per_file):
  payload.root        absolute watch root
  payload.path        path relative to root
  payload.change_type created | modified | deleted

Output: <image>.md next to the image, YAML frontmatter + description body.
Emits image.described (or image.skipped / image.orphan_removed).
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from _response import emit, error, ok

API_URL = "https://api.anthropic.com/v1/messages"
API_VERSION = "2023-06-01"
FALLBACK_BETA = "server-side-fallback-2026-07-01"

MEDIA_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
}

DEFAULT_PROMPT = (
    "Describe this image for a searchable text catalogue. Write two to four "
    "sentences of plain prose. Name every object, part number, label, printed "
    "text, brand, colour and setting you can read or identify, because the "
    "description will be matched against later keyword searches. Do not start "
    "with 'This image shows'."
)

DEFAULTS = {
    "model": "claude-opus-5",
    "effort": "medium",
    "max_tokens": 4096,
    "max_image_bytes": 5 * 1024 * 1024,
    "sidecar_suffix": ".md",
    "secret_name": "anthropic-api-key",
    "delete_orphans": True,
    "timeout_seconds": 120,
    "prompt": DEFAULT_PROMPT,
}


def cfg(config: dict, key: str):
    value = config.get(key)
    return DEFAULTS[key] if value in (None, "") else value


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_frontmatter(sidecar: Path) -> dict:
    """Return the sidecar's frontmatter as a flat dict, or {} if unreadable."""
    try:
        text = sidecar.read_text(encoding="utf-8")
    except OSError:
        return {}
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---\n", 4)
    if end < 0:
        return {}
    meta = {}
    for line in text[4:end].splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip().strip('"')
    return meta


def yaml_str(value: str) -> str:
    return json.dumps(value)  # JSON string literal is valid YAML


def write_sidecar(sidecar: Path, meta: dict, body: str) -> None:
    lines = ["---"]
    for k, v in meta.items():
        lines.append(f"{k}: {yaml_str(v) if isinstance(v, str) else v}")
    lines.append("---")
    lines.append("")
    lines.append(body.strip())
    lines.append("")
    tmp = sidecar.with_name(sidecar.name + ".tmp")
    tmp.write_text("\n".join(lines), encoding="utf-8")
    os.replace(tmp, sidecar)


def call_claude(api_key: str, model: str, effort: str, max_tokens: int,
                prompt: str, media_type: str, data_b64: str, timeout: int) -> str:
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "output_config": {"effort": effort},
        "fallbacks": "default",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64",
                                              "media_type": media_type,
                                              "data": data_b64}},
                {"type": "text", "text": prompt},
            ],
        }],
    }
    req = urllib.request.Request(
        API_URL,
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={
            "content-type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": API_VERSION,
            "anthropic-beta": FALLBACK_BETA,
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        message = json.loads(resp.read().decode("utf-8"))

    stop = message.get("stop_reason")
    if stop == "refusal":
        details = message.get("stop_details") or {}
        raise RuntimeError(f"model refused: {details.get('category')} {details.get('explanation')}")
    text = "".join(b.get("text", "") for b in message.get("content", []) if b.get("type") == "text")
    if not text.strip():
        raise RuntimeError(f"empty response (stop_reason={stop})")
    return text


def handle(config: dict, event: dict, secrets: dict) -> dict:
    payload = event.get("payload", {}) or {}
    root = str(payload.get("root", "")).strip()
    rel = str(payload.get("path", "")).strip()
    change = str(payload.get("change_type", "created")).strip()
    if not root or not rel:
        return error("payload.root and payload.path are required", retry=False)

    image = Path(root) / rel
    suffix = str(cfg(config, "sidecar_suffix"))
    sidecar = image.with_name(image.name + suffix)
    logs = []

    if change == "deleted":
        if cfg(config, "delete_orphans") and sidecar.exists():
            sidecar.unlink()
            return ok(result=f"removed orphan sidecar {sidecar.name}",
                      events=[{"type": "image.orphan_removed",
                               "payload": {"path": str(image), "sidecar_path": str(sidecar)}}])
        return ok(result="deleted image, nothing to do")

    media_type = MEDIA_TYPES.get(image.suffix.lower())
    if not media_type:
        return ok(result=f"skipped {image.name}: unsupported type",
                  events=[{"type": "image.skipped", "payload": {"path": str(image), "reason": "unsupported_type"}}])
    if not image.is_file():
        return error(f"image not found: {image}", retry=False)

    size = image.stat().st_size
    max_bytes = int(cfg(config, "max_image_bytes"))
    if size > max_bytes:
        return ok(result=f"skipped {image.name}: {size} bytes exceeds max_image_bytes={max_bytes}",
                  events=[{"type": "image.skipped", "payload": {"path": str(image), "reason": "too_large", "size": size}}])

    digest = sha256_of(image)
    existing = read_frontmatter(sidecar)
    if existing.get("sha256") == digest:
        return ok(result=f"sidecar for {image.name} is current",
                  events=[{"type": "image.skipped", "payload": {"path": str(image), "reason": "unchanged", "sha256": digest}}])

    api_key = secrets.get(str(cfg(config, "secret_name")))
    if not api_key:
        return error(f"secret {cfg(config, 'secret_name')!r} not delivered", retry=False)

    data_b64 = base64.standard_b64encode(image.read_bytes()).decode("ascii")
    model = str(cfg(config, "model"))
    try:
        description = call_claude(
            api_key, model, str(cfg(config, "effort")), int(cfg(config, "max_tokens")),
            str(cfg(config, "prompt")), media_type, data_b64, int(cfg(config, "timeout_seconds")),
        )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        retry = exc.code == 429 or exc.code >= 500
        return error(f"claude api http {exc.code}: {detail}", retry=retry)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return error(f"claude api unreachable: {exc}", retry=True)
    except RuntimeError as exc:
        return error(str(exc), retry=False)

    mtime = datetime.fromtimestamp(image.stat().st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat()
    meta = {
        "image": image.name,
        "sha256": digest,
        "size_bytes": size,
        "file_modified": mtime,
        "described_at": now_iso(),
        "model": model,
        "generator": "ductile/describe_image",
    }
    write_sidecar(sidecar, meta, description)
    logs.append({"level": "info", "message": f"described {image.name} -> {sidecar.name}"})
    return ok(
        result=f"described {image.name}",
        logs=logs,
        events=[{"type": "image.described",
                 "payload": {"path": str(image), "sidecar_path": str(sidecar),
                             "sha256": digest, "model": model, "change_type": change}}],
    )


def health(config: dict, secrets: dict) -> dict:
    problems = []
    if str(cfg(config, "model")).strip() == "":
        problems.append("model is empty")
    if not secrets.get(str(cfg(config, "secret_name"))):
        problems.append(f"secret {cfg(config, 'secret_name')!r} not delivered")
    if problems:
        return error("; ".join(problems), retry=False)
    return ok(result=f"describe_image OK (model={cfg(config, 'model')})",
              logs=[{"level": "info", "message": "healthy"}])


def main() -> None:
    request = json.loads(sys.stdin.read())
    command = request.get("command", "handle")
    config = request.get("config", {}) or {}
    event = request.get("event", {}) or {}
    secrets = request.get("secrets", {}) or {}

    if command == "health":
        emit(health(config, secrets))
    elif command == "poll":
        emit(ok(result="describe_image is event-driven; poll is a no-op"))
    elif command == "handle":
        emit(handle(config, event, secrets))
    else:
        emit(error(f"unknown command: {command}", retry=False))


if __name__ == "__main__":
    main()
