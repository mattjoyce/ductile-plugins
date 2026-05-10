# marker

Convert PDFs to Markdown by spawning a transient `marker:latest` Docker container per document. Out-of-process, async, CPU-only on the current host.

This plugin is the conversion engine. Its caller is a **Ductile pipeline** (fire-and-forget); the completion signal is a filesystem event on the resulting `.md`, not a status poll. The plugin itself is stateless and sub-second — it never blocks on conversion. Conversion happens in the spawned container; the appearance of `<doc_id>.md` on disk is the definitive "done" event.

For the full ingest architecture, see `~/Projects/Parsem/docs/ductile-pipelines/parsem-ingest.md` — that's the contract Ductile pipelines must honour. This README documents only the marker plugin's interface.

## Quick reference

| Command | Type | Returns | Notes |
|---|---|---|---|
| `submit` | write | `{job_id, state: "running"}` | Spawns a detached Docker container. Returns immediately. |
| `status` | read | `{state, error?, output_path?}` | Available for ad-hoc inspection or fallback polling. NOT used by the production pipeline (filesystem is the truth). |
| `health` | read | `{result: "healthy" \| "degraded"}` | Verifies docker CLI + `marker:latest` image present. |

## Endpoints (via ductile API)

```
POST http://<ductile-host>:8888/plugin/marker/submit
POST http://<ductile-host>:8888/plugin/marker/status   (optional)
POST http://<ductile-host>:8888/plugin/marker/health
```

All require `Authorization: Bearer <token>` and `Content-Type: application/json`. Responses are job envelopes (ductile's async dispatch); read the embedded `result` for the plugin's response.

## `submit` — what PIPELINE 1 calls

**Input:**
```json
{
  "payload": {
    "source": "/mnt/user/Library/parsem-library/inbound/raw/paper.pdf",
    "output_dir": "/mnt/user/Library/parsem-library/inbound/converted/",
    "doc_id": "paper-001"
  }
}
```

| Field | Type | Notes |
|---|---|---|
| `source` | abs path | Path to a PDF on the shared NAS. The plugin bind-mounts it as `/input/in.pdf:ro`. |
| `output_dir` | abs path | Production: `/mnt/user/Library/parsem-library/inbound/converted/`. The container bind-mounts this as `/output`. |
| `doc_id` | string | Caller-chosen stable identifier. Drives output filenames. Comes from Parsem's `/ingest/raw-arrived` response in the production pipeline. |

**Success response:**
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

`state_updates.job_id` is the **Docker container ID**. The PIPELINE 1 caller can discard this — PIPELINE 2's filewatch on `<doc_id>.md` is the completion signal. The container ID is only useful for ad-hoc debugging via `docker logs <id>` or for the `status` endpoint.

**Errors at submit:** missing/invalid input fields, docker daemon unreachable, `marker:latest` image not pullable. Return `{"status": "error", "error": "..."}`. PIPELINE 1 should retry on 5xx (per the parsem-ingest contract); 4xx other than 401 should alert.

## `status` — optional, for ad-hoc inspection

Not used by the production pipeline. Provided for debugging or for a future caller that wants to poll. See [Status semantics](#status-semantics-debugging) at the bottom.

## `health`

**Input:** `{}` (empty payload).

**Response:**
```json
{ "result": { "result": "healthy" | "degraded", "logs": [...] } }
```

`degraded` means either the docker CLI isn't reachable from inside ductile, or `marker:latest` isn't on the host. Conversions will fail until corrected.

## Output shape (filesystem)

After successful conversion, `output_dir` contains:

```
<output_dir>/
├── <doc_id>.md           ← markdown (renamed into place LAST)
├── <doc_id>.json         ← sidecar metadata (renamed into place before .md)
└── <doc_id>_images/      ← extracted images (renamed into place before sidecar)
    ├── _page_0_Picture_0.jpeg
    ├── _page_2_Figure_3.jpeg
    └── in_meta.json      ← marker's own internal metadata (ignore)
```

### Atomic-write contract

The marker container writes in this strict order, with each step atomic:

1. `<doc_id>_images/` directory renamed into place
2. `<doc_id>.json` sidecar renamed into place
3. **`<doc_id>.md` renamed into place — LAST**

PIPELINE 2's filewatch on `*.md` in `inbound/converted/` is reliable because of this guarantee: when the `.md` appears, the images and sidecar are already in place. The plugin **never** writes a partial `.md`. If conversion fails mid-flight, no `.md` ever appears at the final path (only orphaned `.<doc_id>.tmp.<random>/` directories — safe to ignore; `convert.py`'s `finally` block cleans them up on container exit).

### Image references in markdown

Markdown image links are rewritten to include the subdirectory prefix:

```markdown
![](paper-001_images/_page_0_Picture_0.jpeg)
```

Renderers that resolve relative paths will find the image one directory down from the `.md`.

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

Read by Parsem during `/ingest/converted-arrived`. Ductile pipelines do not need to read it.

| Field | Type | Notes |
|---|---|---|
| `doc_id` | string | Same as submit input. |
| `status` | `"ready"` | Always `"ready"` (sidecar is only written on success). |
| `source` / `output_md` / `images_dir` | string | **Container-internal paths** (`/input/...`, `/output/...`). |
| `image_count` | int | Number of image files (jpg/jpeg/png/gif/webp). Excludes marker's `in_meta.json`. |
| `marker_version` | string | Marker-pdf version pinned in the image. |
| `duration_seconds` | float | Wall-clock conversion time. |
| `completed_at` | ISO-8601 UTC | When the sidecar was written. |

## Production caller pattern (PIPELINE 1)

Lifted from `~/Projects/Parsem/docs/ductile-pipelines/parsem-ingest.md`. The pipeline:

```
folderwatch event for inbound/raw/<file>
  ↓
POST parsem:/ingest/raw-arrived  {"path": "<file>"}
  ↓
parse response.action:
  "ingested"        → done
  "duplicate"       → done
  "unsupported"     → done
  "submit_to_marker" → POST marker:/submit with response.{doc_id, source_path}
                      and output_dir = "/mnt/user/Library/parsem-library/inbound/converted/"
                      → discard the returned job_id; PIPELINE 2 will see the .md
```

PIPELINE 2 (separate, filewatch on `inbound/converted/*.md`) tells Parsem when the conversion is done. The marker plugin is not involved in PIPELINE 2.

## Performance characteristics

- **First conversion ever:** ~10-15 min (downloads ~5GB of model weights into the `marker_models` named volume on first run).
- **Subsequent conversions:** ~5-10 min for a 12-page paper, longer for bigger documents (CPU-only, ~30-60s/page).
- **Concurrent conversions:** safe (`concurrency_safe: true`); each spawns its own container with a unique container ID. Multiple in flight will compete for CPU; the container is `--cpus=8` capped (of the box's 12) to leave headroom for other Unraid services.

## Limitations

- **CPU-only.** GPU was tried first; the host's 8GB RTX 2070 is shared with `llama-swap` and contention caused OOMs. The CPU pivot trades speed for reliability. Future hardware upgrade can revert via a one-line Dockerfile change.
- **Tables can be imperfect.** Marker's known limitation. See follow-up issue `claude-8db` for "PDF quality — LLM augmentation" plans (Gemini Flash via marker `--use_llm`, or sibling `gemini-pdf` plugin).
- **No per-document config.** All marker tunables (force_ocr, language hints, paginate, etc.) are baked into the image. If you need a knob, raise an issue against the image.
- **Stateless plugin.** The plugin doesn't remember anything across invocations. The pipeline's `submit` call returns a `job_id` for ad-hoc use, but the production pipeline discards it.

## Status semantics (debugging)

If you ever need `status` (not part of the production pipeline):

```json
// Input
{ "payload": { "job_id": "<container_id>" } }

// Response shape
{
  "result": {
    "result": "running" | "queued" | "ready" | "failed",
    "state_updates": {
      "job_id": "...",
      "state": "running" | "queued" | "ready" | "failed",
      "output_path": "/abs/path/to/<doc_id>.md",
      "error": "...",                              // only on failed
      "error_type": "cuda_oom" | "exit_failure"   // only on failed
    }
  }
}
```

After a terminal state (`ready` or `failed`), the plugin removes the stopped container via `docker rm`. Subsequent `status` calls for the same `job_id` will error.
