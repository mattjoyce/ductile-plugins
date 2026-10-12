# owntracks

Pulls [OwnTracks](https://owntracks.org) points from an edge buffer and appends them to a
local spool, the source of record. Pairs with a receiver that takes the phone's HTTP-mode
posts and keeps them until they are pulled, for example a Cloudflare Worker with D1.

```text
phone (OwnTracks, HTTP mode) ──▶ edge buffer ──GET export?after=<id>──▶ owntracks.poll ──▶ spool
                                                                              │
                                                                              └─▶ location.new_data
```

## What it does

- **`poll`** fetches every row after the cursor, a page at a time, and appends each page to
  `<spool_dir>/<device>/<YYYY-MM>.jsonl` before fetching the next. The month is when the
  buffer received the row. Each line keeps the row's `id`, `user`, `device`, `type`, `tst`,
  `received_at` and the raw `body` exactly as the phone posted it, so encrypted payloads pass
  through untouched. When rows arrived it emits `location.new_data`.
- **`health`** reports whether the spool is writable, its cursor and file count, and whether
  the buffer answers the token and has rows waiting.

## Design notes

- **The cursor is the spool.** It is the highest `id` already written, not plugin state. A
  crash between appending and returning re-pulls nothing that landed and skips nothing that
  didn't. A torn last line from a crash mid-append is cut back on the next run and re-pulled.
  Deleting the spool re-pulls whatever the buffer still holds.
- **Appends are a prefix.** Rows are written in id order and each file is fsynced before
  another is written to, so whatever survives a crash is a prefix of what was pulled.
- **Positions stay in the spool.** Events, logs and results carry counts, ids, devices and
  file names only. Run it with `concurrency_safe: false` and `parallelism: 1`: two pulls at
  once would append the same rows twice.
- **The buffer's duplicates pass through.** OwnTracks can report one fix as several messages
  (a manual publish and a ping with the same `tst`). The spool keeps them all; collapsing is
  the consumer's job.

## The export it expects

`GET <export_url>?after=<id>&limit=<n>` with `Authorization: Bearer <token>`, answering

```json
{"rows": [{"id": 1, "user": "<USER>", "device": "<DEVICE>", "type": "location",
           "tst": 1767225598, "received_at": 1767225600, "body": "<raw JSON string>"}],
 "next_after": 1, "more": false}
```

Rows must come in ascending `id` order after the cursor. 401 or 403 is a permanent error
(fix the token), 5xx, 429 and network failures are retried.

## Configuration

```yaml
plugins:
  owntracks:
    path: /path/to/ductile-plugins/owntracks
    enabled: true
    parallelism: 1
    schedules:
      - id: pull
        command: poll
        every: 15m
        jitter: 1m
    config:
      export_url: https://<HOST>/owntracks/export
      spool_dir: /path/to/owntracks/spool
      # page_limit: 500              rows per request
      # max_pages: 20                requests per poll; the rest waits for the next poll
      # request_timeout_seconds: 30
```

The pull token is a vault secret named `owntracks_pull_token`, granted to this plugin. It
arrives over stdin in `secrets`, never in config or the environment.

## Tests

```bash
cd owntracks && python3 -m unittest test_run
```

They run against a fake export on localhost. Standard library only.
