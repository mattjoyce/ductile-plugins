#!/usr/bin/env python3
"""marker: Wrap the marker:latest Docker image for PDF -> Markdown conversion.

Protocol v2 plugin. Spawns a transient GPU-backed `marker:latest` container
per PDF (`submit`), then polls its lifecycle via `docker inspect` (`status`).

Conversion happens out-of-process inside the marker container. The plugin
itself is always sub-second: it shells out to `docker run -d` (detached) on
submit, and `docker inspect` on status.

The container is invoked as:

    docker run --rm -d \
      -v marker_models:/root/.cache/datalab \
      -v <output_dir>:/output \
      -v <input_pdf>:/input/in.pdf:ro \
      --label marker.output_path=<output_dir>/<doc_id>.md \
      --label marker.doc_id=<doc_id> \
      marker:latest /input/in.pdf /output/<doc_id>.md

Exit 0 + the labelled output_path on disk = `ready`. Anything else = `failed`.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from typing import Any

MARKER_IMAGE = "marker:latest"
MODELS_VOLUME = "marker_models"
MODELS_MOUNT = "/root/.cache/datalab"
LOG_TAIL_LINES = 100

# Cap marker's container at 8 cores (of the box's 12). Marker is async
# background work; leaving 4 cores for ductile, llama-swap, sentinel, etc.
# The image's torch/BLAS env vars match this cap so threads don't
# oversubscribe within the budget.
MARKER_CPU_LIMIT = "8"

# CUDA OOM surfaces as exit-non-zero from the detached marker container, NOT as
# an error on `docker run -d`. We detect it in handle_status by matching the
# container's logs after exit. Callers see error_type="cuda_oom" and decide
# whether to resubmit (typically with backoff while llama-swap evicts).
OOM_PATTERN = re.compile(r"out of memory|CUDA out of memory|OOM", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Response helpers
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
    logs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": True,
        "logs": logs or [{"level": "error", "message": message}],
    }


# ---------------------------------------------------------------------------
# DockerRunner — deep module wrapping the docker CLI
# ---------------------------------------------------------------------------


class DockerRunner:
    """Thin wrapper around the host `docker` CLI.

    The plugin runs inside the ductile container with `/var/run/docker.sock`
    bind-mounted RW, so `docker` invocations here actually run on the host.
    """

    @staticmethod
    def run(
        image: str,
        mounts: list[tuple[str, str, str]],
        args: list[str],
        labels: dict[str, str] | None = None,
    ) -> str:
        """Spawn a detached container and return its container_id.

        `mounts` is a list of (host_or_volume, container_path, mode) tuples
        where mode is e.g. "rw" or "ro" (or "" to omit). The marker image
        is CPU-only on the current Unraid GPU-budget; no `--gpus` flag.
        """
        mount_flags: list[str] = []
        for src, dst, mode in mounts:
            spec = f"{src}:{dst}"
            if mode:
                spec = f"{spec}:{mode}"
            mount_flags.extend(["-v", spec])

        label_flags: list[str] = []
        for key, value in (labels or {}).items():
            label_flags.extend(["--label", f"{key}={value}"])

        cmd = [
            "docker",
            "run",
            "-d",
            "--cpus",
            MARKER_CPU_LIMIT,
            *mount_flags,
            *label_flags,
            image,
            *args,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            raise RuntimeError(stderr or f"docker run exited {proc.returncode}")
        container_id = (proc.stdout or "").strip()
        if not container_id:
            raise RuntimeError("docker run produced no container id on stdout")
        return container_id

    @staticmethod
    def inspect(container_id: str) -> dict[str, Any]:
        """Return the top-level docker inspect dict (NOT just .State).

        Callers need both .State (for status) and .Config.Labels (for the
        marker.output_path label).
        """
        proc = subprocess.run(
            ["docker", "inspect", container_id],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            raise RuntimeError(stderr or f"docker inspect exited {proc.returncode}")
        try:
            payload = json.loads(proc.stdout or "[]")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"docker inspect produced invalid JSON: {exc}") from exc
        if not isinstance(payload, list) or not payload:
            raise RuntimeError(f"docker inspect returned no entries for {container_id}")
        first = payload[0]
        if not isinstance(first, dict):
            raise RuntimeError("docker inspect entry was not an object")
        return first

    @staticmethod
    def image_exists(image: str) -> bool:
        proc = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True,
            text=True,
            check=False,
        )
        return proc.returncode == 0

    @staticmethod
    def rm(container_id: str) -> None:
        """Remove a stopped container. Best-effort; ignore failures."""
        subprocess.run(
            ["docker", "rm", container_id],
            capture_output=True,
            text=True,
            check=False,
        )

    @staticmethod
    def logs(container_id: str, tail: int = LOG_TAIL_LINES) -> str:
        proc = subprocess.run(
            ["docker", "logs", "--tail", str(tail), container_id],
            capture_output=True,
            text=True,
            check=False,
        )
        # docker writes logs to stderr for many images; concatenate both.
        out = (proc.stdout or "") + (proc.stderr or "")
        return out.strip()


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def handle_health() -> dict[str, Any]:
    try:
        present = DockerRunner.image_exists(MARKER_IMAGE)
    except FileNotFoundError:
        return ok_response(
            result="degraded",
            logs=[{"level": "error", "message": "docker CLI not found on PATH"}],
        )
    except OSError as exc:
        return ok_response(
            result="degraded",
            logs=[{"level": "error", "message": f"docker CLI invocation failed: {exc}"}],
        )

    if not present:
        return ok_response(
            result="degraded",
            logs=[
                {
                    "level": "warn",
                    "message": f"docker reachable but image {MARKER_IMAGE} not present on host",
                }
            ],
        )
    return ok_response(
        result="healthy",
        logs=[{"level": "info", "message": f"{MARKER_IMAGE} present on host"}],
    )


def _spawn_marker(
    source: str,
    output_dir: str,
    doc_id: str,
) -> str:
    """Single-shot marker spawn. Raises on docker failure."""
    output_path_in_container = f"/output/{doc_id}.md"
    output_path_on_host = os.path.join(output_dir, f"{doc_id}.md")

    mounts = [
        (MODELS_VOLUME, MODELS_MOUNT, ""),
        (output_dir, "/output", ""),
        (source, "/input/in.pdf", "ro"),
    ]
    labels = {
        "marker.output_path": output_path_on_host,
        "marker.doc_id": doc_id,
    }
    args = ["/input/in.pdf", output_path_in_container]
    return DockerRunner.run(MARKER_IMAGE, mounts, args, labels=labels)


def handle_submit(payload: dict[str, Any]) -> dict[str, Any]:
    source = str(payload.get("source") or "").strip()
    output_dir = str(payload.get("output_dir") or "").strip()
    doc_id = str(payload.get("doc_id") or "").strip()

    missing = [k for k, v in (("source", source), ("output_dir", output_dir), ("doc_id", doc_id)) if not v]
    if missing:
        msg = f"missing required field(s): {', '.join(missing)}"
        return error_response(
            msg,
            logs=[{"level": "error", "message": f"submit: {msg}"}],
        )

    try:
        container_id = _spawn_marker(source, output_dir, doc_id)
    except FileNotFoundError:
        return error_response(
            "docker CLI not found on PATH",
            logs=[{"level": "error", "message": "submit: docker CLI not found"}],
        )
    except (RuntimeError, OSError) as exc:
        return error_response(
            f"marker submit failed: {exc}",
            logs=[{"level": "error", "message": f"submit failed: {exc}"}],
        )

    return ok_response(
        result="running",
        state_updates={"job_id": container_id, "state": "running"},
        logs=[
            {
                "level": "info",
                "message": f"spawned marker container {container_id[:12]} for doc_id={doc_id}",
            }
        ],
    )


def handle_status(payload: dict[str, Any]) -> dict[str, Any]:
    job_id = str(payload.get("job_id") or "").strip()
    if not job_id:
        return error_response(
            "missing required field: job_id",
            logs=[{"level": "error", "message": "status: missing job_id"}],
        )

    try:
        info = DockerRunner.inspect(job_id)
    except FileNotFoundError:
        return error_response(
            "docker CLI not found on PATH",
            logs=[{"level": "error", "message": "status: docker CLI not found"}],
        )
    except (RuntimeError, OSError) as exc:
        return error_response(
            f"docker inspect failed: {exc}",
            logs=[{"level": "error", "message": f"status: inspect failed for {job_id}: {exc}"}],
        )

    state_obj = info.get("State") if isinstance(info.get("State"), dict) else {}
    config_obj = info.get("Config") if isinstance(info.get("Config"), dict) else {}
    labels = config_obj.get("Labels") if isinstance(config_obj.get("Labels"), dict) else {}

    docker_state = str(state_obj.get("Status") or "").lower()
    exit_code = state_obj.get("ExitCode")
    output_path = str(labels.get("marker.output_path") or "")

    if docker_state == "created":
        return ok_response(
            result="queued",
            state_updates={"job_id": job_id, "state": "queued"},
            logs=[{"level": "info", "message": f"{job_id[:12]} queued"}],
        )
    if docker_state == "running":
        return ok_response(
            result="running",
            state_updates={"job_id": job_id, "state": "running"},
            logs=[{"level": "info", "message": f"{job_id[:12]} running"}],
        )
    if docker_state == "exited":
        # The marker container's exit code IS the truth signal. Atomic-write
        # contract on exit 0 guarantees the file is at the labelled path; we
        # don't second-guess it from inside ductile (different filesystem view
        # anyway). Filesystem verification is the caller's concern, not ours.
        if exit_code == 0:
            DockerRunner.rm(job_id)
            return ok_response(
                result="ready",
                state_updates={
                    "job_id": job_id,
                    "state": "ready",
                    "output_path": output_path,
                },
                logs=[
                    {
                        "level": "info",
                        "message": f"{job_id[:12]} ready (output={output_path})",
                    }
                ],
            )
        try:
            tail = DockerRunner.logs(job_id)
        except (FileNotFoundError, RuntimeError, OSError):
            tail = ""
        err = f"marker exited {exit_code}"
        if tail:
            err = f"{err}: {tail}"
        # Tag CUDA OOM specifically so the caller can choose to retry-with-backoff
        # (e.g. wait for llama-swap to TTL-evict) rather than treating it as a
        # permanent failure.
        error_type = "cuda_oom" if tail and OOM_PATTERN.search(tail) else "exit_failure"
        DockerRunner.rm(job_id)
        return ok_response(
            result="failed",
            state_updates={
                "job_id": job_id,
                "state": "failed",
                "error": err,
                "error_type": error_type,
            },
            logs=[{"level": "error", "message": f"{job_id[:12]} failed ({error_type}): {err}"}],
        )

    # Other states (paused, dead, restarting, removing) get reported as-is
    # for visibility. Treat them as failed for pipeline purposes.
    err = f"unexpected docker state: {docker_state or '<empty>'}"
    return ok_response(
        result="failed",
        state_updates={"job_id": job_id, "state": "failed", "error": err},
        logs=[{"level": "warn", "message": f"{job_id[:12]} {err}"}],
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def handle_request(request: dict[str, Any]) -> dict[str, Any]:
    command = str(request.get("command") or "").strip()
    raw_payload = request.get("payload")
    payload: dict[str, Any] = dict(raw_payload) if isinstance(raw_payload, dict) else {}
    # Tolerate top-level fields too (some callers pass input fields directly).
    for key in ("source", "output_dir", "doc_id", "job_id"):
        if key not in payload and key in request:
            payload[key] = request[key]

    if command == "health":
        return handle_health()
    if command == "submit":
        return handle_submit(payload)
    if command == "status":
        return handle_status(payload)
    return error_response(
        f"unknown command: {command}",
        logs=[{"level": "error", "message": f"unknown command: {command}"}],
    )


def main() -> None:
    request = json.load(sys.stdin)
    response = handle_request(request)
    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
