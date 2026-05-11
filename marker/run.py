#!/usr/bin/env python3
"""marker: Wrap the marker:latest Docker image for PDF -> Markdown conversion.

Protocol v2 plugin. Spawns a transient `marker:latest` container per PDF
(`handle`), then polls its lifecycle via `docker inspect` (`status`).

Conversion happens out-of-process inside the marker container. The plugin
itself is always sub-second: it shells out to `docker run -d` (detached) on
handle, and `docker inspect` on status.

The plugin runs *inside* the Ductile container but the `docker run` it issues
is executed by the *host* daemon (the docker socket is bind-mounted), so any
path it names in a `-v`/`--mount` is resolved against the host filesystem, not
Ductile's. To avoid that "two realities" trap entirely, the shared library is a
named Docker *volume* (a daemon-level object — realm-independent): the plugin
mounts it by name, and callers name their input/output paths *inside* that
volume's mountpoint (`/library/...`). No host paths cross the boundary; no
translation is needed.

The container is invoked as:

    docker run --rm -d --cpus 8 \
      --mount type=volume,source=marker_models,target=/root/.cache/datalab \
      --mount type=volume,source=<library_volume>,target=/library \
      --label marker.output_path=<put> \
      --label marker.doc_id=<doc_id> \
      marker:latest <get> <put>

where <get>/<put> are absolute paths *inside* the library volume's mountpoint,
e.g. /library/inbound/raw/foo.pdf and /library/inbound/converted/foo.md.

Exit 0 + the labelled output path on disk = `ready`. Anything else = `failed`.

Input contract for `handle`:
  - get: string  — input path inside /library (the PDF to convert)
  - put: string  — output path inside /library (the .md to produce)
  - doc_id: string (optional — defaults to the basename of `put` without ext;
            only used as a container label for observability)
  - library_volume: string (optional — overrides config; defaults to the
            `library_volume` config key, then to "parsem_library")
  Back-compat: if `get`/`put` are absent, `source`/`output_dir`/`doc_id` are
  accepted and `put` is synthesised as `<output_dir>/<doc_id>.md`, `get` <- `source`.
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
LIBRARY_MOUNT = "/library"
DEFAULT_LIBRARY_VOLUME = "parsem_library"
LOG_TAIL_LINES = 100

# Cap marker's container at 8 cores (of the box's 12). Marker is async
# background work; leaving 4 cores for ductile, llama-swap, sentinel, etc.
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
        mounts: list[tuple[str, str, bool]],
        args: list[str],
        labels: dict[str, str] | None = None,
    ) -> str:
        """Spawn a detached container and return its container_id.

        `mounts` is a list of (volume_name, target_path, readonly) tuples — all
        mounts are named Docker volumes (`--mount type=volume,...`). Using
        `--mount` (not `-v`) means the daemon ERRORS if the named volume does
        not exist, instead of silently creating an empty one — fail loud.
        The marker image is CPU-only on the current Unraid GPU budget; no
        `--gpus` flag.
        """
        mount_flags: list[str] = []
        for vol, target, readonly in mounts:
            spec = f"type=volume,source={vol},target={target}"
            if readonly:
                spec = f"{spec},readonly"
            mount_flags.extend(["--mount", spec])

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
    def reap_exited(image: str) -> list[str]:
        """Remove all *exited* containers spawned from `image`. Best-effort.

        Marker containers aren't run with `--rm` (so `status` can inspect a
        finished one), and the pipeline flow never calls `status` — so exited
        marker containers would otherwise pile up. We sweep them at the start of
        each `handle`. Safe: the conversion output already landed on disk before
        the container exited (atomic-write contract), and a just-finished
        container would have been status-read within seconds, not minutes later
        when a new `handle` arrives. Returns the ids removed.
        """
        try:
            proc = subprocess.run(
                ["docker", "ps", "-aq", "--filter", f"ancestor={image}", "--filter", "status=exited"],
                capture_output=True,
                text=True,
                check=False,
            )
        except (FileNotFoundError, OSError):
            return []
        if proc.returncode != 0:
            return []
        ids = [cid for cid in (proc.stdout or "").split() if cid]
        for cid in ids:
            DockerRunner.rm(cid)
        return ids

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


def _spawn_marker(get_path: str, put_path: str, doc_id: str, library_volume: str) -> str:
    """Single-shot marker spawn. Raises on docker failure.

    Mounts the model cache and the shared library volume; `get_path`/`put_path`
    are absolute paths *inside* the library volume's mountpoint and are passed
    straight through as the marker image's two positional args.
    """
    mounts = [
        (MODELS_VOLUME, MODELS_MOUNT, False),
        (library_volume, LIBRARY_MOUNT, False),  # RW: marker writes the .md/.json/_images under here
    ]
    labels = {
        "marker.output_path": put_path,
        "marker.doc_id": doc_id,
    }
    args = [get_path, put_path]
    return DockerRunner.run(MARKER_IMAGE, mounts, args, labels=labels)


def _resolve_handle_inputs(
    payload: dict[str, Any], config: dict[str, Any]
) -> tuple[str, str, str, str]:
    """Pull (get, put, doc_id, library_volume) from the payload, applying the
    back-compat synthesis from source/output_dir/doc_id."""
    get_path = str(payload.get("get") or payload.get("source") or "").strip()
    put_path = str(payload.get("put") or "").strip()
    doc_id = str(payload.get("doc_id") or "").strip()
    if not put_path:
        output_dir = str(payload.get("output_dir") or "").strip()
        if output_dir and doc_id:
            put_path = os.path.join(output_dir, f"{doc_id}.md")
    if not doc_id and put_path:
        doc_id = os.path.splitext(os.path.basename(put_path))[0]
    library_volume = str(
        config.get("library_volume")
        or payload.get("library_volume")
        or DEFAULT_LIBRARY_VOLUME
    ).strip()
    return get_path, put_path, doc_id, library_volume


def handle_handle(payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    """Event-handler entrypoint — spawns the marker container.

    Named `handle` (not `submit`) because Ductile's pipeline `uses:` routing
    only recognises the standard command names poll/handle/health/init.
    The verb in logs/events is still "submit"/"spawn" for clarity."""
    # Sweep stale exited marker containers from prior conversions before starting
    # a new one (we don't run with --rm so `status` can inspect a finished job;
    # the pipeline flow never calls `status`, so they'd otherwise accumulate).
    reaped = DockerRunner.reap_exited(MARKER_IMAGE)

    get_path, put_path, doc_id, library_volume = _resolve_handle_inputs(payload, config)

    missing = [k for k, v in (("get", get_path), ("put", put_path)) if not v]
    if missing:
        msg = (
            f"missing required field(s): {', '.join(missing)} "
            "(provide get/put, or source/output_dir/doc_id)"
        )
        return error_response(msg, logs=[{"level": "error", "message": f"submit: {msg}"}])

    # Sanity: `get`/`put` are paths inside the library volume — must be absolute,
    # no `..` traversal. (Not a host bind source, so this is malformed-input
    # rejection, not a sandbox escape concern.)
    for name, p in (("get", get_path), ("put", put_path)):
        if not p.startswith("/") or ".." in p.split("/"):
            msg = f"invalid {name} path: {p!r} (must be an absolute path with no '..')"
            return error_response(msg, logs=[{"level": "error", "message": f"submit: {msg}"}])

    extra_logs: list[dict[str, str]] = []
    if reaped:
        extra_logs.append(
            {"level": "info", "message": f"reaped {len(reaped)} exited marker container(s) before submit"}
        )
    if not get_path.startswith(LIBRARY_MOUNT + "/") or not put_path.startswith(LIBRARY_MOUNT + "/"):
        extra_logs.append(
            {
                "level": "warn",
                "message": (
                    f"get/put are not under {LIBRARY_MOUNT}/ — only {LIBRARY_MOUNT} and "
                    f"{MODELS_MOUNT} are mounted in the marker container, so this will likely fail"
                ),
            }
        )

    try:
        container_id = _spawn_marker(get_path, put_path, doc_id, library_volume)
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
        state_updates={"job_id": container_id, "state": "running", "output_path": put_path},
        logs=[
            *extra_logs,
            {
                "level": "info",
                "message": (
                    f"spawned marker container {container_id[:12]} for doc_id={doc_id}: "
                    f"{get_path} -> {put_path} (volume={library_volume})"
                ),
            },
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
            # Report the *realpath* of what was written — honest about where it
            # actually landed (symlinks resolved), not just the path we asked for.
            real_output = os.path.realpath(output_path) if output_path else output_path
            return ok_response(
                result="ready",
                state_updates={
                    "job_id": job_id,
                    "state": "ready",
                    "output_path": real_output,
                },
                logs=[
                    {
                        "level": "info",
                        "message": f"{job_id[:12]} ready (output={real_output})",
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

    # Assemble the input payload from every place Ductile might put it:
    #  1. request["payload"]            — direct HTTP-API callers, raw events
    #  2. request["event"]["payload"]   — `handle` jobs dispatched via a
    #     pipeline: Ductile applies the step's `with:` remap to the *event*
    #     payload, so the remapped fields arrive here, NOT under the top-level
    #     "payload" key (see ductile docs/PLUGIN_DEVELOPMENT.md §2.1).
    #  3. bare top-level fields         — callers that pass inputs flat
    # Later sources win, so a pipeline's `with:` remap overrides the raw event.
    payload: dict[str, Any] = {}
    raw_payload = request.get("payload")
    if isinstance(raw_payload, dict):
        payload.update(raw_payload)
    event = request.get("event")
    if isinstance(event, dict):
        event_payload = event.get("payload")
        if isinstance(event_payload, dict):
            payload.update(event_payload)
    for key in ("get", "put", "source", "output_dir", "doc_id", "job_id", "library_volume"):
        if key not in payload and key in request:
            payload[key] = request[key]

    config = request.get("config") if isinstance(request.get("config"), dict) else {}

    if command == "health":
        return handle_health()
    if command == "handle":
        return handle_handle(payload, config)
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
