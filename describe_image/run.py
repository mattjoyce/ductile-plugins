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

Images larger than max_edge px or max_image_bytes are downscaled with Pillow
(when installed) before upload; the sha256 in the sidecar is always of the original file.

Output: <image>.md next to the image, YAML frontmatter + description body.
Emits image.described (or image.skipped / image.orphan_removed).
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from _response import emit, error, ok

try:  # optional: only needed to downscale oversized images
    from PIL import Image, ImageOps
except ImportError:  # pragma: no cover
    Image = ImageOps = None

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
    "max_image_bytes": 7_000_000,   # raw bytes; base64 of this stays under the API's 10 MB cap
    "max_edge": 2576,               # Opus 5 native long-edge limit; larger is downscaled server-side anyway
    "jpeg_quality": 90,
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
    tmp = sidecar.with_name(f"{sidecar.name}.{os.getpid()}.tmp")
    tmp.write_text("\n".join(lines), encoding="utf-8")
    os.replace(tmp, sidecar)


def image_dimensions(image: Path):
    if Image is None:
        return None
    try:
        with Image.open(image) as im:
            return im.size
    except Exception:
        return None


def prepare_image(image: Path, media_type: str, max_bytes: int, max_edge: int, quality: int):
    """Return (media_type, bytes, note). Downscale with Pillow when the file exceeds
    max_bytes or its long edge exceeds max_edge; otherwise send the file as-is.
    note is None when untouched, a description when downscaled, or a skip reason
    when the image cannot be made to fit."""
    size = image.stat().st_size
    dims = image_dimensions(image)
    too_big = size > max_bytes
    too_wide = bool(dims) and max_edge > 0 and max(dims) > max_edge
    if not (too_big or too_wide):
        return media_type, image.read_bytes(), None
    if Image is None or dims is None:
        return None, None, "too_large" if too_big else "unreadable"

    with Image.open(image) as im:
        im = ImageOps.exif_transpose(im)  # phone photos: bake the rotation in
        im.thumbnail((max_edge, max_edge), Image.LANCZOS)
        if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
            rgba = im.convert("RGBA")
            flat = Image.new("RGB", rgba.size, (255, 255, 255))
            flat.paste(rgba, mask=rgba.split()[-1])
            im = flat
        elif im.mode != "RGB":
            im = im.convert("RGB")
        attempts = []
        if media_type == "image/png":  # keep line art / screenshots lossless if they fit
            attempts.append(("image/png", "PNG", {"optimize": True}))
        for q in (quality, 80, 65):
            attempts.append(("image/jpeg", "JPEG", {"quality": q, "optimize": True}))
        for out_type, fmt, kwargs in attempts:
            buf = io.BytesIO()
            im.save(buf, fmt, **kwargs)
            data = buf.getvalue()
            if len(data) <= max_bytes:
                note = (f"downscaled {dims[0]}x{dims[1]} ({size} B) -> "
                        f"{im.size[0]}x{im.size[1]} {fmt} ({len(data)} B)")
                return out_type, data, note
    return None, None, "too_large_after_downscale"


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
    digest = sha256_of(image)
    existing = read_frontmatter(sidecar)
    if existing.get("sha256") == digest:
        return ok(result=f"sidecar for {image.name} is current",
                  events=[{"type": "image.skipped", "payload": {"path": str(image), "reason": "unchanged", "sha256": digest}}])

    api_key = secrets.get(str(cfg(config, "secret_name")))
    if not api_key:
        return error(f"secret {cfg(config, 'secret_name')!r} not delivered", retry=False)

    send_type, data, note = prepare_image(
        image, media_type, int(cfg(config, "max_image_bytes")),
        int(cfg(config, "max_edge")), int(cfg(config, "jpeg_quality")),
    )
    if data is None:
        return ok(result=f"skipped {image.name}: {note}",
                  events=[{"type": "image.skipped", "payload": {"path": str(image), "reason": note, "size": size}}])
    if note:
        logs.append({"level": "info", "message": f"{image.name}: {note}"})
    data_b64 = base64.standard_b64encode(data).decode("ascii")
    model = str(cfg(config, "model"))
    try:
        description = call_claude(
            api_key, model, str(cfg(config, "effort")), int(cfg(config, "max_tokens")),
            str(cfg(config, "prompt")), send_type, data_b64, int(cfg(config, "timeout_seconds")),
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
    pillow = "pillow=yes" if Image is not None else "pillow=no (oversized images will be skipped)"
    return ok(result=f"describe_image OK (model={cfg(config, 'model')}, {pillow})",
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
