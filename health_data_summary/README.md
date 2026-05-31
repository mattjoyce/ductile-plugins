# health_data_summary

Ductile wrapper plugin that delegates ETL execution to the [`ductile-healthdata`](https://github.com/mattjoyce/ductile-healthdata) Docker image. On a `health.new_data` event, joins Garmin + Withings sources into a unified `daily_health_summary` row in `healthdata.db` and emits `healthdata.etl.completed` for downstream consumers.

## Why a wrapper

The actual integration logic lives in the `ductile-healthdata` image so that:

- The ductile gateway image stays generic — no health-specific Python deps in the gateway runtime
- Integration logic can evolve (add pip deps, change Python version) without rebuilding the gateway
- ETL is testable in isolation without ductile

This plugin is a 45-line bash script that:

1. Reads the ductile request envelope from stdin
2. Resolves `image` / `host_healthdata_dir` / `container_healthdata_dir` from config (with sensible defaults)
3. Bind-mounts the host healthdata tree into the container
4. Pipes the request envelope through `docker run --rm -i ductile-healthdata:latest`
5. Returns the container's stdout as the plugin response

The `ductile-healthdata` image speaks the same ductile v2 protocol (JSON request → JSON response) so the wrapper is fully transparent.

## Pre-flight

Container host must have:
- Docker (CLI + daemon access)
- The `ductile-healthdata` image built and tagged locally (default tag: `ductile-healthdata:latest`; build from `github.com/mattjoyce/ductile-healthdata` via `docker compose build`)

If the ductile gateway is itself running in a container, it needs `docker.sock` mounted (already the case for ductile on Unraid — see `birda` and `blink_sync` for the precedent).

## Commands

| Command | Type | What it does |
|---------|------|--------------|
| `handle` | write | Process `payload.dirty_periods` from a `health.new_data` event, run source-specific ETL (`garmin_etl` or `withings_etl` per `payload.source`), upsert summary + metric rows into `healthdata.db`, emit `healthdata.etl.completed`. Updates state with the post-ETL snapshot. |
| `health` | read | Probe healthdata.db readability + per-source `sync_status` rows + pending update counts. Emits `healthdata_etl.health` with the diagnostic payload (does **not** write state_updates per `PLUGIN_DEVELOPMENT.md §5`). |

## Trigger contract

The plugin is event-driven. Wire it into a pipeline that listens on `health.new_data`:

```yaml
# pipelines.yaml
pipelines:
  - name: health-summary
    on: health.new_data
    steps:
      - id: summarize
        uses: health_data_summary
```

```yaml
# plugins.yaml
plugins:
  health_data_summary:
    enabled: true
    timeout: 120s
    retry:
      max_attempts: 2
    parallelism: 1               # enforce serial dispatch (matches concurrency_safe: false in the manifest)
    config:
      healthdata_db:      /app/data/healthdata/healthdata.db
      garmin_summary_db:  /app/data/healthdata/garmin/DBs/garmin_summary.db
      withings_db:        /app/data/healthdata/withings/withings.db
      # Optional overrides:
      # image:                    ductile-healthdata:latest
      # host_healthdata_dir:      /mnt/user/Projects/healthdata
      # container_healthdata_dir: /app/data/healthdata
```

> **Note**: `concurrency_safe` is a manifest field (declared `false` here because the ETL is a single-writer against `healthdata.db`); enforce it at runtime by pinning `parallelism: 1` in `plugins.yaml`.

## Config keys

| Key | Required | Default | Purpose |
|---|---|---|---|
| `healthdata_db` | yes | — | Path *inside the healthdata container* to the consolidated healthdata.db |
| `garmin_summary_db` | yes | — | Path *inside the healthdata container* to garmin_summary.db |
| `withings_db` | yes | — | Path *inside the healthdata container* to withings.db |
| `image` | no | `ductile-healthdata:latest` | Docker image to invoke |
| `host_healthdata_dir` | no | `/mnt/user/Projects/healthdata` | Host-side path bind-mounted into the container |
| `container_healthdata_dir` | no | `/app/data/healthdata` | Mount target inside the container |

The three required `*_db` paths must resolve inside the bind-mounted `container_healthdata_dir`. Default values assume the conventional Unraid layout.

## Events

| Event | When | Payload |
|---|---|---|
| `healthdata.etl.completed` | `handle` ok | `source`, `periods_processed`, `metrics_written`, `result` |
| `healthdata_etl.health` | `health` (always) | `healthdata_db`, `sources` (array of per-source sync_status rows), `pending_updates` (map of source → count), `checked_at` |

## Durable state

On successful `handle`, the underlying `integrate.py` returns a presence-stable five-key observation snapshot via `state_updates`:

```json
{
  "summary_count": 1247,
  "metric_count": 56340,
  "pending_updates_total": 0,
  "latest_garmin_source_day": "2026-05-30",
  "latest_withings_source_date": "2026-05-30"
}
```

The manifest's `fact_outputs` rule mirrors this into `plugin_facts` as `health_data_summary.snapshot` with the `mirror_object` compatibility view (latest snapshot accessible via `plugin_state`). `health` emits no `state_updates`.

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
- `concurrency_safe: false` declared in manifest; enforced via `parallelism: 1` in `plugins.yaml` ✓
- Wrapper pattern matches sibling plugins (`birda`, `blink_sync`) that also delegate to Docker ✓
