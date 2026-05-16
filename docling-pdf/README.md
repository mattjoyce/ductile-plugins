# docling-pdf

Convert a PDF to Markdown with [IBM docling](https://github.com/DS4SD/docling), then polish the output with a cloud LLM. Replaces the `marker` plugin: docling has far stronger layout/table awareness and runs much faster, and the LLM polish pass repairs the three things PDF extractors get wrong — tables, heading structure, and footnotes/references.

The caller supplies `output_dir`; this plugin writes `<doc_id>.md` there atomically, and a filewatcher on `output_dir` is the completion signal — identical contract to `marker` and `firecrawl`, so the consumer side is unchanged.

## Pipeline

```
source.pdf
  │
  ▼  Stage 1: docling DocumentConverter → raw markdown
  │
  ▼  Stage 2: LLM polish (Gemini default, Claude optional, or skip)
  │             prompt = polish_prompt_v1.txt (versioned artefact)
  ▼
<doc_id>.json  (sidecar, lands first)
<doc_id>.md    (polished markdown, lands LAST — the filewatch trigger)
```

Both stages run in-process. docling and the LLM SDKs are imported lazily so the test suite mocks them without the heavy deps.

## Commands

- `handle` (write): Given `{source, doc_id, output_dir}` in the event payload, parse with docling, polish with the configured LLM, and write `<doc_id>.md` + `<doc_id>.json` to `output_dir` atomically. Emits `content_ready`.
- `health` (read): Verifies docling is importable and the configured provider's API key is present.

## Configuration

All keys optional. With no config the plugin defaults to Gemini and will error at `handle` time if `gemini_api_key` is unset.

| Key | Default | Description |
|---|---|---|
| `llm_provider` | `gemini` | `gemini` \| `claude` \| `none` (parse-only, skip polish) |
| `gemini_api_key` | — | Required when `llm_provider=gemini` |
| `gemini_model` | `gemini-2.5-pro` | Gemini model id |
| `anthropic_api_key` | — | Required when `llm_provider=claude` |
| `anthropic_model` | `claude-sonnet-4-6` | Claude model id |
| `polish_temperature` | `0.0` | Low temp keeps polish deterministic-leaning |
| `llm_timeout_seconds` | `120` | Per-request LLM timeout |

`none` exists for diagnostics and cost control — it writes docling's raw output untouched and records `polish_skipped_reason` in the sidecar.

## Payload

```json
{
  "payload": {
    "source":     "/abs/path/to/document.pdf",
    "doc_id":     "42",
    "output_dir": "/path/to/watched/output/dir"
  }
}
```

`doc_id` must be a safe filename component — no path separators, no leading dot, no `..`.

## Outputs

**`<doc_id>.md`** — polished markdown. Lands LAST.

**`<doc_id>.json`** — sidecar:

```json
{
  "doc_id":                  "42",
  "status":                  "ready",
  "docling_pdf_version":     "0.1.0",
  "docling_version":         "2.x.y",
  "llm_provider":            "gemini",
  "llm_model":               "gemini-2.5-pro",
  "polish_prompt_version":   "v1",
  "page_count":              17,
  "parse_duration_seconds":  4.231,
  "polish_duration_seconds": 9.874,
  "polish_skipped_reason":   null,
  "source":                  "/abs/path/to/document.pdf",
  "started_at":              "<ISO timestamp>",
  "completed_at":            "<ISO timestamp>"
}
```

## Events

`content_ready` — payload references the file, not its content:

```json
{
  "doc_id":                  "42",
  "output_path":             "<absolute path to .md>",
  "page_count":              17,
  "parse_duration_seconds":  4.231,
  "polish_duration_seconds": 9.874
}
```

## Error handling

| Class | `retry` | Examples |
|---|---|---|
| Config / bad input | `false` | Missing API key, bad `doc_id`, `source` not found, `output_dir` missing |
| docling parse failure | `false` | Corrupt/unsupported PDF — won't convert on retry |
| Transient LLM error | `true` | 5xx, 429, timeout, connection reset |
| Permanent LLM error | `false` | 4xx / auth / empty response |
| Write failure | `true` | Disk full, permission flip after pre-check |

## Deployment

The dependency tree (docling → torch/transformers/layout models) is heavy. Two models, decided at deploy time (`claude-fro` slice 3):

- **A — in-ductile-container** (firecrawl model): deps installed into ductile's own image; ductile runs `run.py` in-process. Simplest if ductile's image can carry docling.
- **B — sidecar image** (marker model): build this `Dockerfile` on unraid; ductile shells into the container per job. Isolates the heavy deps. Swap the torch wheel for a CUDA build to use the RTX 2070.

`requirements.txt` is the authoritative dependency list for either model.

## Manual test

```bash
echo '{
  "command": "handle",
  "config": {"gemini_api_key": "<KEY>"},
  "event": {"payload": {"source": "/abs/in.pdf", "doc_id": "t1", "output_dir": "/tmp"}}
}' | python3 run.py
```

## Run tests

```bash
python3 -m unittest test_run.py -v
```

The real-docling integration test (`TestRealDoclingIntegration`) skips automatically unless docling is installed and `tests/fixtures/sample.pdf` exists.

## Sanitization

Public repo. No tokens, host paths, or instance names are hard-coded — all flow in via plugin config or the per-request payload.
