# health_data_summary

Ductile wrapper plugin that delegates ETL execution to the [`healthdata`](https://github.com/mattjoyce/healthdata) Docker image. On a `health.new_data` event, joins Garmin + Withings sources into a unified daily row in `summary.db` and emits `health.summary_updated` for downstream consumers (weekly report, dashboards, etc.).

## Why a wrapper

The actual integration logic (Python, ~600 lines) lives in the `healthdata` image so that:

- The ductile gateway image stays generic — no health-specific Python deps in the gateway runtime
- Integration logic can evolve (add pip deps, change Python version) without rebuilding the gateway
- ETL is testable in isolation (`bash test/smoke.sh` in the healthdata repo) without ductile

This plugin is a 30-line bash script that:

1. Reads the ductile request envelope from stdin
2. Resolves `image` / `host_healthdata_dir` / `container_healthdata_dir` from config (with sensible defaults)
3. Bind-mounts the host healthdata tree into the container
4. Pipes the request envelope through `docker run --rm -i healthdata:latest`
5. Returns the container's stdout as the plugin response

The `healthdata` image speaks the same ductile v2 protocol (JSON request → JSON response) so the wrapper is fully transparent.

## Pre-flight

Container host must have:
- Docker (CLI + daemon access)
- The `healthdata` image built and tagged locally (`healthdata:latest` by default; build from `github.com/mattjoyce/healthdata` via `docker compose build && docker tag healthdata:0.1.0 healthdata:latest`)

If the ductile gateway is itself running in a container, it needs `docker.sock` mounted (already the case for ductile on Unraid — see `birda` and `blink_sync` for the precedent).

## Commands

| Command | Type | What it does |
|---------|------|--------------|
| `summarize` | read | Process `dirty_periods` from event payload, upsert one row per day into `daily_health_summary`, emit `health.summary_updated` |
| `query`     | read | Return rows from `daily_health_summary` in a date range, emit `health.query_result` |
| `health`    | read | Probe DB readability + image availability. **Emits no `state_updates`.** |

## Trigger contract

The plugin is event-driven. Wire it into a pipeline that listens on `health.new_data`:

```yaml
# plugins.yaml
plugins:
  health_data_summary:
    enabled: true
    timeout: 60s
    max_attempts: 2
    concurrency_safe: false
    config:
      garmin_db_path: /app/data/healthdata/garmin/garmin.db
      withings_db_path: /app/data/healthdata/withings/withings.db
      summary_db_path: /app/data/healthdata/summary/summary.db
      # Optional overrides:
      # image: healthdata:0.1.0
      # host_healthdata_dir: /mnt/user/Projects/healthdata
      # container_healthdata_dir: /app/data/healthdata

# pipelines.yaml
pipelines:
  - name: health-summary
    on: health.new_data
    steps:
      - id: summarize
        uses: health_data_summary
```

## Config keys

| Key | Required | Default | Purpose |
|---|---|---|---|
| `garmin_db_path` | yes | — | Path *inside the healthdata container* to garmin.db |
| `withings_db_path` | yes | — | Path *inside the healthdata container* to withings.db |
| `summary_db_path` | yes | — | Path *inside the healthdata container* to summary.db |
| `image` | no | `healthdata:latest` | Docker image to invoke |
| `host_healthdata_dir` | no | `/mnt/user/Projects/healthdata` | Host-side path bind-mounted into the container |
| `container_healthdata_dir` | no | `/app/data/healthdata` | Mount target inside the container |

The three required `*_db_path` values must resolve inside the bind-mounted `container_healthdata_dir`. Default values assume the conventional Unraid layout.

## Durable state

On successful `summarize`, the underlying `integrate.py` emits a presence-stable two-key snapshot via `state_updates`:

```json
{
  "latest_summarized_day": "2026-04-22",
  "summary_row_count": 42
}
```

The manifest's `fact_outputs` rule mirrors this into `plugin_facts` as `health_data_summary.snapshot`. `health` and `query` emit no `state_updates`.

## Observability

`run.sh` short-circuits with a clear error if:
- The `docker` CLI isn't on the gateway PATH
- The configured `image` isn't built locally

Errors are returned as ductile error responses with `retry: false` so the gateway doesn't loop.

## Doctrine alignment

- protocol 2 ✓
- `values.consume` / `values.emit` per command ✓
- `health` emits no `state_updates` ✓
- snapshot is observed-state, presence-stable ✓
- `fact_outputs` rule for `health_data_summary.snapshot` ✓
- `concurrency_safe: false` (single-writer durable resource = `summary.db`) ✓
- Wrapper pattern matches sibling plugins (`birda`, `blink_sync`) that also delegate to Docker ✓
