# marker

Convert PDFs to Markdown by spawning a transient `marker:latest` Docker container per document. Out-of-process, async, CPU-only on the current host.

This plugin is the seam between a caller (e.g. Parsem) and the `marker:latest` image deployed on Unraid. The plugin itself is stateless and sub-second — it never blocks on conversion. Conversion happens in the spawned container; the caller polls `status` to discover progress.

## Quick reference

| Command | Type | Returns | Notes |
|---|---|---|---|
| `submit` | write | `{job_id, state: "running"}` | Spawns a detached Docker container. Returns immediately. |
| `status` | read | `{state, error?, error_type?, output_path?}` | Reads `docker inspect`. Cleans up the container after a terminal state. |
| `health` | read | `{result: "healthy" \| "degraded"}` | Verifies docker CLI + `marker:latest` image present. |

## Lifecycle (caller's perspective)

```
caller                              marker plugin             marker:latest container
──────                              ─────────────             ──────────────────────
submit(source, output_dir, doc_id)
  ───────────────►
                                    docker run -d ─────────────►  spawned, detached
  ◄─── {job_id, "running"}                                        (~5-10 min CPU work)
                                                                  …
                                                                  …
                                                                  exits 0, .md on disk

[poll every ~5-30s]
status(job_id)
  ───────────────►
                                    docker inspect ────────────►  reads container state
  ◄─── {"running"} (or "ready"/"failed")
                                    docker rm (on terminal state)
```

## Endpoints (via ductile API)

```
POST http://<ductile-host>:8888/plugin/marker/submit
POST http://<ductile-host>:8888/plugin/marker/status
POST http://<ductile-host>:8888/plugin/marker/health
```

All require `Authorization: Bearer <token>` and `Content-Type: application/json`. Responses are job envelopes (ductile's async dispatch); poll `GET /job/<uuid>` to read the plugin's response.

## `submit`

**Input:**
```json
{
  "payload": {
    "source": "/abs/path/to/input.pdf",
    "output_dir": "/abs/path/to/library",
    "doc_id": "paper-001"
  }
}
```

| Field | Type | Notes |
|---|---|---|
| `source` | abs path | Must be readable by the `marker:latest` container. The plugin bind-mounts it as `/input/in.pdf:ro`. |
| `output_dir` | abs path | Must be writable. The container bind-mounts it as `/output`. The library NAS share at `/mnt/user/Library/parsem-library/` is the production path. |
| `doc_id` | string | Caller-chosen stable identifier. Drives the output filenames. |

**Success response (after polling `/job/<uuid>`):**
```json
{
  "result": {
    "result": "running",
    "state_updates": {
      "job_id": "21c092b2da6af0db986703969908966fdabdb3af8c099df63e4e3d8195be0d42",
      "state": "running"
    }
  }
}
```

`state_updates.job_id` is the **Docker container ID**. Caller holds this and passes it back to `status`.

**Errors:** missing/invalid input fields, docker daemon unreachable, `marker:latest` image not pullable. All return `{"status": "error", "error": "..."}` immediately. No retries.

## `status`

**Input:**
```json
{
  "payload": {
    "job_id": "21c092b2da6af0db986703969908966fdabdb3af8c099df63e4e3d8195be0d42"
  }
}
```

**Response (after polling `/job/<uuid>`):**
```json
{
  "result": {
    "result": "running" | "queued" | "ready" | "failed",
    "state_updates": {
      "job_id": "...",
      "state": "running" | "queued" | "ready" | "failed",
      "output_path": "/abs/path/to/library/paper-001.md",
      "error": "...",          // only on failed
      "error_type": "cuda_oom" | "exit_failure"  // only on failed
    }
  }
}
```

**State semantics:**

| `state` | Meaning | Caller action |
|---|---|---|
| `queued` | Container created but not yet running. Brief; transient. | Poll again. |
| `running` | Container is converting. Expect 5-10 min on CPU for a 12-page paper. | Poll every ~30s. |
| `ready` | Container exited 0. Output file exists per the marker contract. The plugin removed the container. | Read the markdown + sidecar (see below). Stop polling. |
| `failed` | Container exited non-zero, OR docker inspect failed. `error` describes; `error_type` classifies. The plugin removed the container. | Surface `error` to user; consider `error_type` for retry logic. |

**`error_type` classifications:**

| Value | When | Caller hint |
|---|---|---|
| `cuda_oom` | Container logs match `out of memory \| CUDA out of memory \| OOM`. | Retry with backoff after waiting for the GPU to free up. (Currently CPU-only, so this won't fire — kept for future GPU mode.) |
| `exit_failure` | Container exited non-zero for any other reason. | Surface to user; do not auto-retry. |

## `health`

**Input:** `{}` (empty payload).

**Response:**
```json
{ "result": { "result": "healthy" | "degraded", "logs": [...] } }
```

`degraded` means either the docker CLI isn't reachable from inside ductile, or `marker:latest` isn't on the host. Conversions will fail until corrected.

## Output shape (filesystem)

After `state: "ready"`, the `output_dir` contains:

```
<output_dir>/
├── <doc_id>.md           ← markdown (with image refs rewritten to point at the subdir)
├── <doc_id>.json         ← sidecar metadata (see schema below)
└── <doc_id>_images/      ← extracted images
    ├── _page_0_Picture_0.jpeg
    ├── _page_2_Figure_3.jpeg
    └── in_meta.json      ← marker's own internal metadata (ignore)
```

### Atomic-write contract (important for filesystem watchers)

The marker container writes in this strict order, with each step atomic:

1. `<doc_id>_images/` directory renamed into place
2. `<doc_id>.json` sidecar renamed into place
3. **`<doc_id>.md` renamed into place — LAST**

A filesystem watcher waiting on `.md` is guaranteed that when the `.md` appears, the images and JSON are already in place. The plugin **never** writes a partial `.md`. If conversion fails mid-flight, no `.md` ever appears at the final path (only orphaned `.<doc_id>.tmp.<random>/` directories which can be safely cleaned up).

### Image references in markdown

Markdown image links are rewritten to include the subdirectory prefix:

```markdown
![](paper-001_images/_page_0_Picture_0.jpeg)
```

Renderers that resolve relative paths (most do) will find the image one directory down from the `.md`.

## Sidecar JSON schema (`<doc_id>.json`)

```json
{
  "doc_id": "paper-001",
  "status": "ready",
  "source": "/input/in.pdf",
  "output_md": "/output/paper-001.md",
  "images_dir": "/output/paper-001_images",
  "image_count": 3,
  "marker_version": "1.10.2",
  "duration_seconds": 599.519,
  "completed_at": "2026-05-10T10:25:55.612512+00:00"
}
```

| Field | Type | Notes |
|---|---|---|
| `doc_id` | string | Same as submit input. |
| `status` | `"ready"` | Always `"ready"` (sidecar is only written on success). |
| `source` / `output_md` / `images_dir` | string | **Container-internal paths** (`/input/...`, `/output/...`). Reflects what the container saw. The host paths the caller used are not preserved here — caller already knows them. |
| `image_count` | int | Number of image files (jpg/jpeg/png/gif/webp). Excludes `in_meta.json`. |
| `marker_version` | string | Marker-pdf version pinned in the image. |
| `duration_seconds` | float | Wall-clock conversion time. |
| `completed_at` | ISO-8601 UTC | When the sidecar was written. |

## Recommended caller pattern

For a callable like Parsem that wants both responsiveness and reliability:

```
1. Add document → state=converting
2. POST /plugin/marker/submit → store job_id
3. EITHER:
   a) Filesystem watch on output_dir for <doc_id>.md appearing
      → on detect, read <doc_id>.json for metadata, flip state=ready
   b) Poll /plugin/marker/status every 30s with job_id
      → on state="ready", flip state=ready
4. On failure (state="failed"):
   - error_type="cuda_oom" → wait + resubmit
   - error_type="exit_failure" → surface error; manual retry only
```

The filesystem-watch path (3a) is preferred because:
- Lower API load (no polling)
- Direct from the filesystem source of truth (atomic-write contract guarantees no false positives)
- Sidecar JSON gives metadata richer than the API response

The polling path (3b) is the backup for when filesystem watch can't be set up (e.g., over SMB on a Mac dev machine where fsevents is unreliable).

## Performance characteristics

- **First conversion ever:** ~10-15 min (downloads ~5GB of model weights into the `marker_models` named volume on first run).
- **Subsequent conversions:** ~5-10 min for a 12-page paper, longer for bigger documents (CPU-only, ~30-60s/page).
- **Concurrent conversions:** safe (`concurrency_safe: true`); each spawns its own container with a unique container ID. Multiple in flight will compete for CPU.

## Limitations

- **CPU-only.** GPU was tried first; the host's 8GB RTX 2070 is shared with `llama-swap` and contention caused OOMs. The CPU pivot trades speed for reliability. Future hardware upgrade can revert via a one-line Dockerfile change.
- **Tables can be imperfect.** Marker's known limitation. See `claude-08g`'s follow-up issue `claude-8db` for "PDF quality — LLM augmentation" plans (Gemini Flash via marker `--use_llm`, or sibling `gemini-pdf` plugin).
- **No per-document config.** All marker tunables (force_ocr, language hints, paginate, etc.) are baked into the image. If you need a knob, raise an issue against the image, not the plugin.
- **Stateless.** The plugin doesn't remember anything across invocations. The caller must hold the `job_id` for the lifetime of a conversion.
