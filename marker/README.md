# marker

Convert PDFs to Markdown by spawning a transient `marker:latest` GPU container per document.

## Plugin Facts

This plugin declares no `fact_outputs`. Each invocation is a one-shot
control of an out-of-process Docker container; the durable artefact (the
generated `.md` and its image folder) lives on disk, not in `plugin_state`.
Callers track jobs by holding the `job_id` (the Docker container id)
returned from `submit` and polling `status`.

## Commands
- `submit` (write): Spawn `marker:latest` detached on the host. Returns `job_id` (container id) and `state: "running"`. Retries up to 3 times on CUDA/OOM errors with 60s/120s/240s backoff.
- `status` (read): Inspect the container and report `queued | running | ready | failed`. `ready` requires both exit code 0 and the labelled output file existing on disk.
- `health` (read): Verify the docker CLI is reachable and `marker:latest` is present on the host.

## Configuration

None. The plugin is intentionally narrow — all marker tunables (force_ocr,
language hints, etc.) are baked into the `marker:latest` image.

## Container Contract

`submit` invokes:

```
docker run --rm -d --gpus all \
  -v marker_models:/root/.cache/datalab \
  -v <output_dir>:/output \
  -v <source>:/input/in.pdf:ro \
  --label marker.output_path=<output_dir>/<doc_id>.md \
  --label marker.doc_id=<doc_id> \
  marker:latest /input/in.pdf /output/<doc_id>.md
```

The `marker_models` named volume is shared across runs to avoid re-downloading
model weights. The marker container performs its own atomic write — the
output `.md` only appears on disk on success.

## Example

```yaml
plugins:
  marker:
    enabled: true
```

```bash
echo '{"command":"submit","payload":{"source":"/data/library/paper.pdf","output_dir":"/data/library/markdown","doc_id":"paper-001"}}' \
  | python3 run.py
# {"status":"ok","result":"running","state_updates":{"job_id":"<container_id>","state":"running"}, ...}

echo '{"command":"status","payload":{"job_id":"<container_id>"}}' \
  | python3 run.py
# {"status":"ok","result":"ready","state_updates":{"job_id":"...","state":"ready","output_path":"/data/library/markdown/paper-001.md"}, ...}
```
