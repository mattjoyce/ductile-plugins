# docling-pdf

Gateway-side plugin that calls the `ductile-docling` HTTP satellite to convert
a PDF to Markdown. Synchronous: POSTs `{input_path, output_path}` to
`<satellite_url>/convert` and blocks until the satellite writes the `.md` and
returns 200.

Drops the marker plugin from the Parsem ingest pipeline. The satellite
(`ductile-docling`, see [`unraid_admin/ductile-docling`](../../unraid_admin/ductile-docling))
is reached over the LAN at `http://192.168.20.4:8889` on the standard Unraid
deploy.

## Commands

- `handle` — convert one PDF. Reads `source_path` (input PDF) and `doc_id` (used
  to derive `output_path` when not given). Returns `result=ready` and
  state-updates carrying the realised `output_path`, `page_count`,
  `parse_duration_seconds`, and `docling_version`.
- `health` — GET the satellite's `/healthz`; reports `healthy` / `degraded`.

## Pipeline use

```yaml
- name: parsem-needs-docling
  on: parsem.needs_docling
  steps:
    - id: convert
      uses: docling-pdf
      with:
        source_path: "{payload.source_path}"   # e.g. /library/originals/42/source.pdf
        doc_id:      "{payload.doc_id}"        # e.g. "42"
```

`output_path` defaults to `<output_dir>/<doc_id>.md` where `output_dir` is
the plugin config key (default `/library/inbound/converted`). The
`parsem_converted_watch` folder watcher then sees the new `.md` and POSTs
to Parsem's `/ingest/converted-arrived`.

## Path contract

`source_path` and `output_path` are absolute paths visible **inside the
satellite container**. On the standard deploy the `parsem_library` bind volume
(`/mnt/user/Library/parsem-library` on the host) is mounted at `/library` in
both the gateway-side marker container and the docling satellite, so a
`/library/...` path resolves identically in both.

## Plugin config

```yaml
docling-pdf:
  enabled: true
  timeout: 3600s            # docling on CPU is slow — up to ~30 min for a book
  max_attempts: 2
  config:
    satellite_url: "http://192.168.20.4:8889"
    output_dir: "/library/inbound/converted"
    request_timeout: 3600
```

## Failure semantics

| satellite response | plugin result        | retry? |
|--------------------|----------------------|--------|
| 200                | `ok / ready`         | —      |
| 400                | `error`              | no     |
| 503                | `error`              | yes    |
| connection error   | `error` (unreachable)| yes    |
