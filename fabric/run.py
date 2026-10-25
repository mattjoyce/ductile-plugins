#!/usr/bin/env python3
"""Fabric AI wrapper plugin for Ductile Gateway.

Wraps the fabric CLI tool to process text through predefined patterns.
Protocol v2: reads JSON from stdin, writes JSON to stdout.

Opt-in additions (0.3.0), each off unless configured or sent:
- FABRIC_ENV / FABRIC_HOME: run fabric with its settings from config, not a file in
  the gateway's home. fabric reads its vendor settings from the environment, but refuses
  to start without a ~/.config/fabric/.env, so an empty one is created in FABRIC_HOME.
- FABRIC_RAW: pass --raw, for OpenAI-compatible servers that mishandle chat options.
- FABRIC_TIMEOUT_SECONDS: how long one run may take (default 120).
- FABRIC_FILE_ROOT: the one folder input_file and output_file may name a file under.
  Without it both are refused, since a path in an event could otherwise point anywhere.
- payload.input_file: read the input text from a file, not the event.
- payload.output_format "json" (or FABRIC_OUTPUT_FORMAT): the output must be one JSON
  value; a surrounding Markdown code fence is removed, and the value is emitted as
  payload.data. Output that is not JSON fails the run.
- payload.output_file: also write the output there, whole or not at all.
"""
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

# A run fails fast rather than hanging; 120 s was the fixed limit before it was a key.
DEFAULT_TIMEOUT_SECONDS = 120
# An input file larger than this is refused rather than sent to a model whole.
DEFAULT_MAX_INPUT_BYTES = 1_000_000
# One fenced block, optionally labelled (```json), around the whole output.
_FENCE = re.compile(r"^```[A-Za-z0-9_-]*[ \t]*\n(.*?)\n?```$", re.DOTALL)


def as_bool(value):
    return value is True or str(value).strip().lower() in ("1", "true", "yes", "on")


def fabric_env(config):
    """The environment fabric runs in: the gateway's, plus FABRIC_ENV, with HOME set to
    FABRIC_HOME when given. Returns None to inherit the gateway's unchanged."""
    extra = config.get("FABRIC_ENV")
    home = config.get("FABRIC_HOME")
    if not extra and not home:
        return None
    if extra is not None and not isinstance(extra, dict):
        raise ValueError("FABRIC_ENV must be a map of variable names to values")
    env = dict(os.environ)
    for key, value in (extra or {}).items():
        env[str(key)] = "" if value is None else str(value)
    if home:
        env["HOME"] = str(home)
        settings = os.path.join(str(home), ".config", "fabric", ".env")
        if not os.path.exists(settings):
            os.makedirs(os.path.dirname(settings), exist_ok=True)
            open(settings, "a").close()
    return env


def parse_json_output(output):
    """The model's output as a JSON value, a surrounding code fence removed."""
    text = output.strip()
    fenced = _FENCE.match(text)
    if fenced:
        text = fenced.group(1).strip()
    return json.loads(text)


def confine(path, root):
    """path resolved, and refused unless it lies under root. Symlinks are resolved first,
    so a link cannot lead out of root."""
    if not root:
        raise ValueError("set FABRIC_FILE_ROOT to use input_file or output_file")
    real_root = os.path.realpath(str(root))
    real = os.path.realpath(os.path.join(real_root, str(path)))
    if os.path.commonpath([real_root, real]) != real_root or real == real_root:
        raise ValueError(f"{path} is outside FABRIC_FILE_ROOT")
    return real


def write_atomic(path, text):
    folder = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".fabric-", dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def read_input_file(path, limit):
    """At most limit bytes of a regular file. Opened without following a final symlink
    and without blocking, then checked on the open descriptor, so a FIFO, device or
    swapped-in link cannot stall or redirect the read."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(fd, "rb") as fh:
        if not stat.S_ISREG(os.fstat(fh.fileno()).st_mode):
            raise ValueError("input_file is not a regular file")
        data = fh.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f"input_file is over the {limit} byte limit")
    return data.decode("utf-8", errors="replace")


def pick(payload, context, key, default=None):
    if isinstance(payload, dict) and key in payload and payload[key] not in (None, ""):
        return payload[key]
    if isinstance(context, dict) and key in context and context[key] not in (None, ""):
        return context[key]
    return default


def poll_command(config, state):
    """Poll command - fabric is event-driven; poll is a diagnostic no-op and does not own durable state."""
    return {
        "status": "ok",
        "result": "Fabric poll command - no scheduled actions configured",
        "logs": [
            {"level": "info", "message": "Fabric poll command - no scheduled actions configured"},
        ],
    }


def handle_command(config, state, event, context):
    """Handle command - processes events with fabric patterns"""
    payload = event.get("payload", {})

    # Extract input parameters (payload first, context fallback)
    text = pick(payload, context, "text") or pick(payload, context, "content")
    input_file = pick(payload, context, "input_file")
    output_file = pick(payload, context, "output_file")
    output_format = str(pick(payload, context, "output_format") or config.get("FABRIC_OUTPUT_FORMAT") or "text").lower()
    if output_format not in ("text", "json"):
        return error_response(f"output_format must be text or json, not {output_format!r}")
    try:
        timeout = int(config.get("FABRIC_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))
        max_input = int(config.get("FABRIC_MAX_INPUT_BYTES", DEFAULT_MAX_INPUT_BYTES))
        env = fabric_env(config)
    except (TypeError, ValueError, OSError) as exc:
        return error_response(f"Bad fabric config: {exc}")
    if timeout < 1 or max_input < 1:
        return error_response("FABRIC_TIMEOUT_SECONDS and FABRIC_MAX_INPUT_BYTES must be at least 1")
    file_root = config.get("FABRIC_FILE_ROOT")
    try:
        input_path = confine(input_file, file_root) if input_file else None
        output_path = confine(output_file, file_root) if output_file else None
    except ValueError as exc:
        return error_response(f"Refused file path: {exc}")
    if input_path and not text:
        try:
            text = read_input_file(input_path, max_input)
        except (OSError, ValueError) as exc:
            return error_response(f"Cannot read input_file: {exc}")
    url = pick(payload, context, "url")
    youtube_url = pick(payload, context, "youtube_url")

    # Only use default pattern if pattern not explicitly provided AND not in prompt-only mode
    # Prompt-only mode: prompt provided without text/url/youtube_url and without explicit pattern
    pattern = pick(payload, context, "pattern")
    if pattern is None and not (pick(payload, context, "prompt") and not any([text, url, youtube_url])):
        pattern = config.get("FABRIC_DEFAULT_PATTERN")

    prompt = pick(payload, context, "prompt") or config.get("FABRIC_DEFAULT_PROMPT")
    model = pick(payload, context, "model") or config.get("FABRIC_DEFAULT_MODEL")
    fabric_bin = config.get("FABRIC_BIN_PATH", "fabric")

    # Validate input: need at least one of text, url, youtube_url, or prompt
    if not any([text, url, youtube_url, prompt]):
        return error_response("Missing input: provide 'text', 'url', 'youtube_url', or 'prompt'")

    # Build fabric command
    # Support multiple modes:
    # 1) pattern mode: fabric --pattern <preset>
    # 2) prompt mode: no pattern required; prompt is prepended to text input
    # 3) URL mode: fabric --scrape_url=<url>
    # 4) YouTube mode: fabric --youtube=<url>
    cmd = [fabric_bin]

    if pattern:
        cmd.extend(["--pattern", pattern])
    if model:
        cmd.extend(["--model", model])
    if url:
        cmd.extend(["--scrape_url", url])
    if youtube_url:
        cmd.extend(["--youtube", youtube_url])
    if as_bool(config.get("FABRIC_RAW")):
        cmd.append("--raw")

    # Prepare input text
    # If using URL/YouTube, fabric fetches content automatically
    # If using text, optionally prepend prompt
    input_text = ""
    if text:
        input_text = text
        if prompt:
            input_text = f"{prompt}\n\n{text}"
    elif prompt and not (url or youtube_url):
        # One-shot question mode: just a prompt with no text/URL
        input_text = prompt
    elif prompt and (url or youtube_url):
        # URL/YouTube with prompt: fabric will fetch content, we prepend prompt
        input_text = prompt

    try:
        result = subprocess.run(
            cmd,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except FileNotFoundError:
        return error_response(
            f"Fabric binary not found at '{fabric_bin}'. "
            "Install with: go install github.com/danielmiessler/fabric@latest",
            retry=False,
        )
    except subprocess.TimeoutExpired:
        return error_response(f"Fabric execution timed out after {timeout}s", retry=True)

    if result.returncode != 0:
        stderr = result.stderr.strip()
        return error_response(f"Fabric failed (exit {result.returncode}): {stderr}", retry=True)

    output = result.stdout.strip()
    data = None
    if output_format == "json":
        try:
            data = parse_json_output(output)
        except ValueError as exc:
            # A model may answer differently next time, so this is worth a retry.
            return error_response(f"Fabric output is not JSON: {exc}", retry=True)
    if output_file:
        try:
            body = json.dumps(data, indent=2, ensure_ascii=False) + "\n" if output_format == "json" else output + "\n"
            write_atomic(output_path, body)
        except OSError as exc:
            return error_response(f"Cannot write output_file: {exc}", retry=True)
    executions_count = state.get("executions_count", 0) + 1

    # Build event payload with fabric results
    event_payload = {
        "result": output,
        "pattern": pattern or "",
        "prompt": prompt or "",
        "model": model or "default",
        "url": url or "",
        "youtube_url": youtube_url or "",
        "input_length": len(text) if text else 0,
        "output_length": len(output),
    }
    if output_format == "json":
        event_payload["data"] = data
    if input_file:
        event_payload["input_file"] = input_file
    if output_file:
        event_payload["output_file"] = output_file

    # Propagate pipeline context fields for downstream steps
    # (e.g., output_dir, output_path, filename from upstream plugins)
    for field in ["output_dir", "output_path", "filename", "file_path", "folder"]:
        value = pick(payload, context, field)
        if value is not None and value != "":
            event_payload[field] = value

    log_message = build_log_message(pattern, url, youtube_url, prompt)
    return {
        "status": "ok",
        "result": log_message,
        "events": [
            {
                "type": "fabric.completed",
                "payload": event_payload,
            }
        ],
        "state_updates": {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "executions_count": executions_count,
            "last_pattern": pattern or "",
            "last_prompt": prompt or "",
        },
        "logs": [
            {
                "level": "info",
                "message": log_message,
            },
        ],
    }


def health_command(config):
    fabric_bin = config.get("FABRIC_BIN_PATH", "fabric")
    try:
        env = fabric_env(config)
    except (TypeError, ValueError, OSError) as exc:
        return error_response(f"Bad fabric config: {exc}")

    try:
        result = subprocess.run(
            [fabric_bin, "--help"],
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
        )
    except FileNotFoundError:
        return error_response(
            f"Fabric binary not found at '{fabric_bin}'. "
            "Install with: go install github.com/danielmiessler/fabric@latest",
            retry=False,
        )
    except subprocess.TimeoutExpired:
        return error_response("Fabric health check timed out", retry=False)

    if result.returncode != 0:
        return error_response(f"Fabric --help failed: {result.stderr.strip()}", retry=False)

    # Count available patterns
    pattern_count = 0
    try:
        list_result = subprocess.run(
            [fabric_bin, "--listpatterns"],
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
        )
        if list_result.returncode == 0:
            pattern_count = len([line for line in list_result.stdout.strip().splitlines() if line.strip()])
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    return {
        "status": "ok",
        "result": f"Fabric healthy, {pattern_count} patterns available",
        "logs": [
            {"level": "info", "message": f"Fabric healthy, {pattern_count} patterns available"},
        ],
    }


def build_log_message(pattern, url, youtube_url, prompt):
    """Build descriptive log message based on execution mode"""
    parts = []
    if pattern:
        parts.append(f"pattern={pattern}")
    if url:
        parts.append(f"url={url[:50]}...")
    if youtube_url:
        parts.append(f"youtube={youtube_url[:50]}...")
    if prompt and not (url or youtube_url):
        parts.append("prompt-mode")

    mode = ", ".join(parts) if parts else "text-mode"
    return f"Executed fabric ({mode})"


def error_response(message, retry=False):
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": [{"level": "error", "message": message}],
    }


def main():
    request = json.load(sys.stdin)
    command = request.get("command", "")
    config = request.get("config", {})
    state = request.get("state", {})
    event = request.get("event", {})
    context = request.get("context", {})

    if command == "poll":
        response = poll_command(config, state)
    elif command == "handle":
        response = handle_command(config, state, event, context)
    elif command == "health":
        response = health_command(config)
    else:
        response = error_response(f"Unknown command: {command}")

    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
