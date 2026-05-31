# fabric

Wrapper around the [`fabric`](https://github.com/danielmiessler/fabric) CLI for text processing and summarisation. Routes inbound text / URLs / YouTube links through a chosen pattern (or freeform prompt) and emits the result as `fabric.completed`. Stateful — maintains a running `executions_count` plus last-run metadata via `fact_outputs`.

## Commands

- `poll` (write): Diagnostic no-op. Event-driven plugin; poll exists for periodic liveness exercises without side effects.
- `handle` (write): Run one fabric invocation. Returns the captured output as an event.
- `health` (read): Verify the fabric binary is callable and count available patterns via `fabric --listpatterns`.

## Configuration

All optional:

| Key | Default | Purpose |
|---|---|---|
| `FABRIC_BIN_PATH` | `fabric` | Path to the fabric binary on the gateway PATH |
| `FABRIC_DEFAULT_PATTERN` | — | Pattern used when `payload.pattern` is absent and not in prompt-only mode |
| `FABRIC_DEFAULT_PROMPT` | — | Prompt used when `payload.prompt` is absent |
| `FABRIC_DEFAULT_MODEL` | — | Model used when `payload.model` is absent |

## Input (`handle`)

Payload fields (all optional, but at least one of `text`/`url`/`youtube_url`/`prompt` is required):

| Field | Purpose |
|---|---|
| `text` / `content` | Raw text to run through the pattern |
| `url` | URL for `fabric --scrape_url` |
| `youtube_url` | YouTube URL for `fabric --youtube` |
| `pattern` | Named fabric pattern (overrides `config.FABRIC_DEFAULT_PATTERN`) |
| `prompt` | Freeform prompt; prepended to text input, or used alone in prompt-only mode |
| `model` | Model override (overrides `config.FABRIC_DEFAULT_MODEL`) |
| `output_dir` / `output_path` / `filename` / `file_path` | Pipeline-context fields, forwarded into the `fabric.completed` event payload for downstream steps |

## Events

Emits `fabric.completed` with payload: `result`, `pattern`, `prompt`, `model`, `url`, `youtube_url`, `input_length`, `output_length`, plus the four pipeline-context fields above when present.

## Durable state

On successful `handle`, returns `state_updates` with:

```json
{
  "last_run": "<iso-8601>",
  "executions_count": <int>,
  "last_pattern": "<pattern>",
  "last_prompt": "<prompt>"
}
```

The manifest's `fact_outputs` rule snapshots this as `fabric.snapshot` (mirror_object). The `executions_count` is the rolling tally and is read back on the next invocation — without fact_outputs the counter would silently stay at 1 (the gateway discards state_updates when no rule matches).

## Example

```yaml
plugins:
  fabric:
    enabled: true
    config:
      FABRIC_DEFAULT_PATTERN: summarize
      FABRIC_DEFAULT_MODEL: gpt-4o
```
