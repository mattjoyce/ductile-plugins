# firecrawl

Scrape URLs via the [Firecrawl API](https://firecrawl.dev) and atomically write the resulting markdown to a caller-specified directory. Designed as the URL-side companion to filesystem-based ingest pipelines — the caller supplies `output_dir`, this plugin writes `<doc_id>.md` there atomically, and a filewatcher on `output_dir` is the completion signal.

## Architectural note — file-write vs event-emit

Ductile's idiomatic shape for URL scrapers is event-emission (see `jina-reader`: emit `content_ready` with content in payload, let a pipeline step route it). This plugin instead writes the markdown to disk and emits an event that *references* the file path. The trade-off:

| | Event-emit (jina-reader) | File-write (this plugin) |
|---|---|---|
| Plugin idiom | Pure | Slightly braided — plugin owns a filesystem destination |
| Caller integration | Caller must consume events | Caller can consume via filewatch (existing pattern) |
| Suited for | Pipelines that route content through multiple transforms | Pipelines that already use filewatch as a seam (e.g. PDF converters writing to a shared NAS path) |

The plugin still emits a `content_ready` event so it participates in ductile's observability — the event payload references the output path rather than carrying the content.

## Commands

- `handle` (write): Given `{url, doc_id, output_dir}` in the event payload, scrape the URL via Firecrawl and write `<doc_id>.md` plus `<doc_id>.json` sidecar to `output_dir` atomically. Emits `content_ready`. (Named `handle` per ductile protocol v2 — the gateway only populates `event` for that command name.)
- `health` (read): Returns health status; fails if `firecrawl_api_key` is not configured.

## Configuration

| Key | Required | Default | Description |
|---|---|---|---|
| `firecrawl_api_key` | yes | — | Firecrawl API key (`fc-...`) |
| `api_base_url` | no | `https://api.firecrawl.dev/v1` | Override for testing or self-hosted |
| `timeout_seconds` | no | `60` | Per-request timeout |

## Scrape payload

```json
{
  "payload": {
    "url":        "https://example.com/article",
    "doc_id":     "ab12cd34ef56",
    "output_dir": "/path/to/watched/output/dir"
  }
}
```

`doc_id` must be a safe filename component — no path separators, no leading dot, no `..`. The plugin refuses anything else.

## Outputs

Two files appear in `output_dir`:

**`<doc_id>.md`** — the scraped markdown. Lands LAST.

**`<doc_id>.json`** — sidecar metadata:

```json
{
  "doc_id":            "ab12cd34ef56",
  "status":            "ready",
  "url":               "<original URL submitted>",
  "final_url":         "<URL after redirects, from Firecrawl>",
  "firecrawl_version": "0.1.0",
  "duration_seconds":  12.345,
  "status_code":       200,
  "started_at":        "<ISO timestamp>",
  "completed_at":      "<ISO timestamp>",
  "content_hash":      "<16-char sha256 prefix>",
  "title":             "<page title if available>",
  "description":       "<meta description if available>"
}
```

The atomic-write contract: sidecar is renamed first, markdown last. A filewatcher on `*.md` always sees both files in place when the trigger fires.

## Events

`content_ready` — emitted on successful scrape. Payload:

```json
{
  "url":              "<original URL>",
  "doc_id":           "<doc_id>",
  "output_path":      "<absolute path to .md>",
  "content_hash":     "<16-char sha256 prefix>",
  "duration_seconds": 12.345
}
```

The content itself is *not* in the event payload — it lives in the file. Consumers that want the content should read `output_path`.

## Error handling

| Class | `retry` flag | Examples |
|---|---|---|
| Config error | `false` | Missing `firecrawl_api_key`, malformed payload |
| Bad input | `false` | Invalid `doc_id`, `output_dir` doesn't exist |
| Permanent upstream error | `false` | Firecrawl 4xx (bad URL, auth failure) |
| Transient upstream error | `true` | Firecrawl 5xx, network timeout, connection reset |
| Invalid response shape | `false` | Firecrawl returned non-success or no markdown |
| Write failure | `true` | Filesystem full, permission flip after pre-check |

The plugin follows ductile's let-it-crash discipline: errors are surfaced honestly via `status: "error"`, and `retry: true|false` tells the supervisor whether to re-queue.

## Example pipeline config

```yaml
plugins:
  firecrawl:
    enabled: true
    config:
      firecrawl_api_key: ${FIRECRAWL_API_KEY}
      timeout_seconds: 60
```

Then trigger via the gateway API:

```bash
curl -X POST http://<HOST>:<PORT>/plugin/firecrawl/handle \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"payload": {"url": "https://example.com", "doc_id": "abc123", "output_dir": "/path/to/output"}}'
```

## Manual test

```bash
echo '{
  "command": "handle",
  "config": {"firecrawl_api_key": "<YOUR_KEY>"},
  "event": {"payload": {"url": "https://example.com", "doc_id": "test1", "output_dir": "/tmp"}}
}' | python3 run.py
```

## Run tests

```bash
python3 -m unittest test_run.py -v
```

## Sanitization

This plugin lives in a public repo. The implementation hard-codes no tokens, no host-specific paths, and no instance names. All such values flow in via plugin config or the per-request payload.
