# birdnet_firstday

Poll a BirdNET-Go detections database and emit one event per species the first
time it is detected on the current local calendar day.

## Plugin Facts

`poll` declares `birdnet_firstday.snapshot` as a fact output from
`state_updates`. Ductile records that snapshot append-only in `plugin_facts`
and keeps `plugin_state` as the compatibility/current-view row via
`mirror_object`.

The poll snapshot is:

```json
{
  "watermark": 42,
  "last_polled_at": "2026-04-24T00:00:00+00:00",
  "species_cache": {
    "Tyto alba": "Barn Owl"
  },
  "species_cache_fetched_at": "2026-04-24T00:00:00+00:00"
}
```

If `species_url` is unset, the snapshot omits the species cache fields and the
plugin operates in scientific-name-only mode.

The species cache stays in the snapshot deliberately. In protocol v2 there is
no separate durable cache channel apart from plugin state, and this lookup map
is reused on subsequent polls to preserve enrichment behavior without forcing a
network round-trip every tick.

## Commands

- `poll` (write): Query today's detections, emit one event per newly observed
  first-of-day species, and return the full compatibility snapshot.
- `health` (read): Report database reachability, today's detection count, and
  current watermark without mutating durable state.

## Configuration

- `db_path`: BirdNET-Go SQLite database path.
- `species_url`: Optional BirdNET-Go species list endpoint used to map
  scientific names to common names.
- `event_type`: Event type to emit. Defaults to `birdnet.firstday_species`.
- `cache_ttl_seconds`: Species cache refresh TTL in seconds.
- `http_timeout_seconds`: HTTP timeout for species list fetches.

## Events

Emits `birdnet.firstday_species` by default with payload fields including
`scientific_name`, `common_name`, `first_id`, `first_time`, `peak_conf`,
`detected_at`, and `message`.
