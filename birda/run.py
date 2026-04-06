#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""birda — Ductile plugin (protocol v2).

Runs the birda Docker container on Unraid to perform GPU-accelerated bird
species detection using BirdNET v24. The plugin executes:

    docker run --rm --gpus all -v /mnt/user:/mnt/user birda \
        --gpu -m birdnet-v24 -f raven --output-mode ndjson \
        --no-progress --force \
        <wav_path> --lat <lat> --lon <lon> [--week <week>] [-c <min_conf>]

Input (event.payload):
    wav_path  (str)   — absolute host path to WAV file (/mnt/user/...)
    lat       (float) — latitude for range filtering
    lon       (float) — longitude for range filtering
    min_conf  (float) — minimum confidence threshold (default: 0.7)
    week      (int)   — week number 1–48 for range filter (-1 to disable)

Output (response):
    output_path      — path to .BirdNET.selection.table.txt written by birda
    detections       — list of {start_s, end_s, scientific_name, common_name, confidence}
    duration_s       — audio duration in seconds
    realtime_factor  — realtime processing factor (e.g. 379.5)
    detection_count  — total number of detections above threshold

State: none
Events emitted: none
"""
from __future__ import annotations

import csv
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

DOCKER_BIN_CANDIDATES = ["/usr/bin/docker", "docker"]
DEFAULT_MIN_CONF = 0.7
BIRDA_TIMEOUT_S = 300


def ok(result: str, logs: list[dict] | None = None, **extra: Any) -> dict[str, Any]:
    resp: dict[str, Any] = {
        "status": "ok",
        "result": result,
        "logs": logs or [{"level": "info", "message": result}],
    }
    resp.update(extra)
    return resp


def err(message: str, *, retry: bool = False, logs: list[dict] | None = None) -> dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": logs or [{"level": "error", "message": message}],
    }


def find_docker() -> str | None:
    for candidate in DOCKER_BIN_CANDIDATES:
        if shutil.which(candidate) or Path(candidate).is_file():
            return candidate
    return None


def parse_raven_table(output_path: Path) -> list[dict[str, Any]]:
    if not output_path.is_file():
        return []
    detections: list[dict[str, Any]] = []
    with open(output_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, dialect="excel-tab")
        for row in reader:
            try:
                det = {
                    "start_s": float(row.get("Begin Time (s)", row.get("start_s", 0))),
                    "end_s": float(row.get("End Time (s)", row.get("end_s", 0))),
                    "scientific_name": row.get("Scientific name", row.get("scientific_name", "")).strip(),
                    "common_name": row.get("Common Name", row.get("Common name", row.get("common_name", ""))).strip(),
                    "confidence": float(row.get("Confidence", row.get("confidence", 0))),
                }
                detections.append(det)
            except (ValueError, KeyError):
                pass
    detections.sort(key=lambda d: d["start_s"])
    return detections


def parse_birda_stats(output: str) -> tuple[float | None, float | None]:
    """Extract duration_s and realtime_factor from birda ndjson output.

    birda emits spec_version 1.0 ndjson with an "event" key:
      {"spec_version":"1.0","event":"pipeline_completed","payload":{"duration_ms":N,"realtime_factor":F,...}}
    """
    duration_ms = None
    realtime_factor = None

    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("event") == "pipeline_completed":
            payload = event.get("payload", {})
            duration_ms = payload.get("duration_ms")
            realtime_factor = payload.get("realtime_factor")
            break

    duration_s = duration_ms / 1000.0 if duration_ms is not None else None

    # Fallback regex for non-ndjson or unexpected output format
    if realtime_factor is None:
        m = re.search(r"([\d.]+)x\s+realtime", output, re.IGNORECASE)
        if m:
            try:
                realtime_factor = float(m.group(1))
            except ValueError:
                pass
    if duration_s is None:
        m = re.search(r"in\s+([\d.]+)s\b", output, re.IGNORECASE)
        if m:
            try:
                duration_s = float(m.group(1))
            except ValueError:
                pass

    return duration_s, realtime_factor


def _get_coord(payload: dict, context: dict, key: str) -> float:
    raw = payload.get(key) if payload.get(key) is not None else context.get(key)
    return float(raw)


def handle_handle(req: dict[str, Any]) -> dict[str, Any]:
    event = req.get("event") or {}
    payload = event.get("payload") or {}
    context = req.get("context") or {}
    config = req.get("config") or {}

    wav_path = payload.get("wav_path") or context.get("wav_path", "")
    if not wav_path:
        return err("wav_path is required in event.payload", retry=False)

    try:
        lat = _get_coord(payload, context, "lat")
    except (TypeError, ValueError):
        return err("lat must be a number", retry=False)
    try:
        lon = _get_coord(payload, context, "lon")
    except (TypeError, ValueError):
        return err("lon must be a number", retry=False)

    default_min_conf = float(config.get("default_min_conf", DEFAULT_MIN_CONF))
    raw_conf = payload.get("min_conf")
    min_conf = float(raw_conf) if raw_conf is not None else default_min_conf

    raw_week = payload.get("week")
    week = int(raw_week) if raw_week is not None else -1

    docker_bin = config.get("docker_bin") or find_docker()
    if not docker_bin:
        return err("docker binary not found; mount /usr/bin/docker into the container", retry=False)

    cmd = [
        docker_bin, "run", "--rm", "--gpus", "all",
        "-v", "/mnt/user:/mnt/user",
        "birda",
        "--gpu",
        "-m", "birdnet-v24",
        "-f", "raven",
        "--output-mode", "ndjson",
        "--no-progress",
        "--force",
        wav_path,
        "--lat", str(lat),
        "--lon", str(lon),
        "-c", str(min_conf),
    ]
    if week != -1:
        cmd += ["--week", str(week)]

    logs: list[dict[str, str]] = [
        {"level": "info", "message": f"analyzing {Path(wav_path).name}"},
        {"level": "debug", "message": f"command: {' '.join(cmd)}"},
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=BIRDA_TIMEOUT_S,
        )
    except FileNotFoundError:
        return err(f"docker binary not executable: {docker_bin}", retry=False, logs=logs)
    except subprocess.TimeoutExpired:
        logs.append({"level": "error", "message": f"birda timed out after {BIRDA_TIMEOUT_S}s"})
        return err(f"birda timed out after {BIRDA_TIMEOUT_S}s", retry=True, logs=logs)

    combined_output = proc.stdout + "\n" + proc.stderr

    if proc.returncode != 0:
        logs.append({"level": "error", "message": f"docker exited {proc.returncode}"})
        if proc.stderr:
            logs.append({"level": "error", "message": f"stderr: {proc.stderr[:4096]}"})
        return err(f"birda exited with code {proc.returncode}", retry=False, logs=logs)

    wav = Path(wav_path)
    output_path = wav.parent / f"{wav.stem}.BirdNET.selection.table.txt"

    detections = parse_raven_table(output_path)
    duration_s, realtime_factor = parse_birda_stats(combined_output)

    summary = (
        f"{len(detections)} detections in {wav.name}"
        + (f" — {duration_s:.1f}s audio" if duration_s else "")
        + (f", {realtime_factor:.0f}x realtime" if realtime_factor else "")
    )
    logs.append({"level": "info", "message": summary})

    return ok(
        summary,
        logs=logs,
        output_path=str(output_path),
        detections=detections,
        duration_s=duration_s,
        realtime_factor=realtime_factor,
        detection_count=len(detections),
    )


def handle_health(req: dict[str, Any]) -> dict[str, Any]:
    config = req.get("config") or {}
    docker_bin = config.get("docker_bin") or find_docker()

    if not docker_bin:
        return err("docker binary not found; mount /usr/bin/docker into the container", retry=False)

    try:
        version_proc = subprocess.run(
            [docker_bin, "version", "--format", "{{.Client.Version}}"],
            capture_output=True, text=True, timeout=10,
        )
        docker_version = version_proc.stdout.strip() if version_proc.returncode == 0 else "unknown"
    except Exception:
        docker_version = "unreachable"

    try:
        images_proc = subprocess.run(
            [docker_bin, "image", "ls", "--format", "{{.Repository}}:{{.Tag}}", "birda"],
            capture_output=True, text=True, timeout=10,
        )
        birda_present = bool(images_proc.stdout.strip()) if images_proc.returncode == 0 else False
    except Exception:
        birda_present = False

    if not birda_present:
        return err(
            "birda image not found; build it on Unraid: cd /mnt/user/appdata/birda && docker build -t birda .",
            retry=False,
        )

    return ok(
        f"docker {docker_version} available; birda image present",
        logs=[{"level": "info", "message": f"docker {docker_version} reachable; birda image found"}],
    )


def main() -> int:
    try:
        req = json.load(sys.stdin)
    except Exception as exc:
        json.dump(err(f"invalid request json: {exc}"), sys.stdout)
        sys.stdout.write("\n")
        return 0

    if not isinstance(req, dict):
        json.dump(err("request must be a JSON object"), sys.stdout)
        sys.stdout.write("\n")
        return 0

    command = str(req.get("command", "")).strip()

    if command == "handle":
        resp = handle_handle(req)
    elif command == "health":
        resp = handle_health(req)
    else:
        resp = err(f"unsupported command: {command!r}", retry=False)

    json.dump(resp, sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
