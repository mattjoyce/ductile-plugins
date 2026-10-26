#!/usr/bin/env python3
"""pdf_text: turns a PDF into a text file beside it, for any pipeline to chain.

Protocol v2: reads JSON from stdin, writes JSON to stdout.

handle: payload.pdf (or payload.input_file) names a PDF under config.root. pdftotext reads
it and the text is written to <pdf>.txt, or to payload.output_file under the root, whole or
not at all. pdf_text.completed carries input_file (the text path) and pdf (the PDF), plus
every other payload field unchanged, so the next step sees what the first one was sent.
A PDF with no text layer still emits, with empty true, so a later step can route it to OCR.

Config:
- root    : the one folder a PDF or output file may live under. Required to handle; without
            it every path is refused, since a path in an event could point anywhere.
- layout  : pass -layout (default true)
- timeout : seconds one pdftotext run may take (default 60)
"""
import json
import os
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

DEFAULT_TIMEOUT = 60
# What this plugin sets itself; a forwarded payload field never overrides them.
OWN_FIELDS = ("input_file", "pdf", "empty", "output_file")


def as_bool(value):
    return value is True or str(value).strip().lower() in ("1", "true", "yes", "on")


def confine(path, root):
    """path resolved, and refused unless it lies under root. Symlinks are resolved first,
    so a link cannot lead out of root."""
    if not root:
        raise ValueError("set root to name a file")
    real_root = os.path.realpath(str(root))
    real = os.path.realpath(os.path.join(real_root, str(path)))
    if os.path.commonpath([real_root, real]) != real_root or real == real_root:
        raise ValueError(f"{path} is outside root")
    return real


def check_regular(path):
    """Refuse anything but a regular file, judged on an open descriptor opened without
    following a final link or blocking, so a FIFO or swapped-in link cannot stall the run."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("pdf is not a regular file")
    finally:
        os.close(fd)


def write_atomic(path, text):
    folder = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".pdf_text-", dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def error_response(message, retry=False):
    return {"status": "error", "error": message, "retry": retry,
            "logs": [{"level": "error", "message": message}]}


def handle_command(config, state, event, context):
    payload = event.get("payload") or {}
    context = context or {}

    def pick(key):
        for src in (payload, context):
            if isinstance(src, dict) and src.get(key) not in (None, ""):
                return src[key]
        return None

    try:
        timeout = int(config.get("timeout", DEFAULT_TIMEOUT))
    except (TypeError, ValueError):
        return error_response("Bad config: timeout must be a whole number of seconds")
    if timeout < 1:
        return error_response("Bad config: timeout must be at least 1")
    layout = as_bool(config.get("layout", True))
    root = config.get("root")

    named = pick("pdf") or pick("input_file")
    if not named:
        return error_response("Missing input: provide 'pdf' (or 'input_file')")
    output_file = pick("output_file")
    try:
        pdf = confine(named, root)
        check_regular(pdf)
        out = confine(output_file, root) if output_file else pdf + ".txt"
    except (ValueError, OSError) as exc:
        return error_response(f"Refused file path: {exc}")

    cmd = ["pdftotext"] + (["-layout"] if layout else []) + ["-enc", "UTF-8", pdf, "-"]
    try:
        done = subprocess.run(cmd, capture_output=True, timeout=timeout, check=False)
    except FileNotFoundError:
        return error_response("pdftotext not found on the path (install poppler-utils)", retry=True)
    except subprocess.TimeoutExpired:
        return error_response(f"pdftotext timed out after {timeout}s", retry=True)
    if done.returncode != 0:
        return error_response(f"pdftotext exited {done.returncode}: "
                              f"{done.stderr.decode('utf-8', 'replace').strip()[:200]}")

    text = done.stdout.decode("utf-8", errors="replace")
    try:
        write_atomic(out, text)
    except OSError as exc:
        return error_response(f"Cannot write {out}: {exc.strerror or type(exc).__name__}", retry=True)

    empty = text.strip() == ""
    # The payload as sent, then what this step adds. Both paths are real paths, resolved
    # under root, so a later step compares like with like.
    event_payload = {k: v for k, v in payload.items() if k not in OWN_FIELDS}
    event_payload.update({"input_file": out, "pdf": pdf, "empty": empty})
    message = f"Extracted {os.path.basename(pdf)}" + (" (no text layer)" if empty else "")
    return {
        "status": "ok",
        "result": message,
        "events": [{"type": "pdf_text.completed", "payload": event_payload}],
        "state_updates": {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "executions_count": state.get("executions_count", 0) + 1,
            "last_pdf": os.path.basename(pdf),
            "last_empty": empty,
        },
        "logs": [{"level": "info", "message": message}],
    }


def health_command(config):
    try:
        done = subprocess.run(["pdftotext", "-v"], capture_output=True, timeout=10, check=False)
    except FileNotFoundError:
        return error_response("pdftotext not found on the path (install poppler-utils)")
    except subprocess.TimeoutExpired:
        return error_response("pdftotext -v timed out")
    if done.returncode != 0:
        return error_response(f"pdftotext -v exited {done.returncode}")
    root = config.get("root")
    if not root or not os.path.isdir(str(root)):
        return error_response("root is not set or is not a folder")
    message = "pdftotext present, root exists"
    return {"status": "ok", "result": message, "logs": [{"level": "info", "message": message}]}


def main():
    request = json.load(sys.stdin)
    command = request.get("command", "")
    config = request.get("config") or {}
    if command == "handle":
        response = handle_command(config, request.get("state") or {}, request.get("event") or {},
                                  request.get("context") or {})
    elif command == "health":
        response = health_command(config)
    else:
        response = error_response(f"Unknown command: {command}")
    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
