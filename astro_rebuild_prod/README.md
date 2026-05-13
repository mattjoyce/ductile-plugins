# astro_rebuild_prod

Rebuilds and restarts the production [mattjoyce.ai](https://mattjoyce.ai) Astro
site via Docker Compose, in response to an HMAC-signed webhook.

If you are new to ductile, you only need to know one thing to start: **this
plugin is a thin wrapper around a single shell command that you, the operator,
configure in `plugins.yaml`**. The webhook is just the trigger. The plugin
does not look at the request body — it only checks that the body was signed
with a shared secret, then runs your configured command.

---

## What this plugin does

Triggered by `POST /webhook/mattjoyce-publish` on the ductile webhook listener,
this plugin:

1. Verifies the request signature (HMAC-SHA256, header `X-Ductile-Signature-256`,
   `sha256=HEX` value).
2. Runs the operator-configured `command` (typically `docker compose build && up -d`)
   from the operator-configured `working_dir` (typically the matt_joyce compose
   project on the Unraid host, made visible to the ductile container via a
   read-only bind mount).
3. Emits an `mattjoyce.publish.completed` event carrying the exit code,
   duration, captured stdout/stderr, and timing — visible in `ductile job logs`.
4. Returns a `job_id` so the caller can inspect the run later.

That's it. There is no payload contract. The request body is the trigger,
not the program.

---

## Relationship to `astro_rebuild_staging`

This plugin is a **sibling clone** of `astro_rebuild_staging`, not a fork.
`run.py` is byte-identical to its source. The differences are deliberately
narrow and live entirely in the manifest and the deploy-side YAML:

| | `astro_rebuild_staging` | `astro_rebuild_prod` |
|---|---|---|
| Host | ThinkPad (LAN) | Unraid (prod) |
| Trigger | `folder_watch` on `site/src/content/summaries/` | HMAC-signed webhook |
| Default emitted event | `sys_exec.completed` | `mattjoyce.publish.completed` |
| Audience | "the author's staging site" | "the author's published site" |
| Failure blast radius | None (LAN-only staging) | Public site |

If you find yourself diff'ing the two `run.py` files and they are different,
something has drifted and one of them is wrong. The intent is identical
implementations with distinct *names* (because the operator-facing surface
should make it obvious which one you're staring at in a log).

---

## Prerequisites

Before adding this plugin, the ductile instance must already have:

1. **Webhook listener enabled** in `config.yaml` (`webhooks:` block with a
   `listen:` address — typically `0.0.0.0:8091`). If you don't have this yet,
   see `docs/WEBHOOKS.md` in the core ductile repo.
2. **`webhooks.yaml` and `tokens.yaml`** included from `config.yaml`. These
   are high-security files; ductile refuses to start if their checksums don't
   match `.checksums`.
3. **Docker socket bind-mounted** into the ductile container
   (`/var/run/docker.sock:/var/run/docker.sock`). This plugin shells out to
   the host docker daemon via this socket.
4. **The compose project you intend to rebuild** must be bind-mounted into
   the ductile container at the same host path you'll use in `working_dir`.
   Read-only is sufficient — `docker-cli` inside ductile only needs to *read*
   the compose file; the daemon (running on the host) handles build context.
5. **A docker-cli inside the ductile image**. The stock ductile Dockerfile
   already installs `docker-cli`. If you've customized, verify
   `docker exec ductile docker --version` works.

---

## Quick start (operator setup)

> **Where am I editing this?** All YAML edits below happen **on the Unraid
> host** (`ssh root@<unraid>`), in `/mnt/user/appdata/ductile/config/`. Do
> NOT edit via the NAS Samba mount — the canonical Unraid deploy runbook
> makes that "pull-only" because SMB oplocks block follow-up operations.
> The *plugin source* (`astro_rebuild_prod/`) is a different story — that
> lives under `/mnt/user/Projects/ductile-plugins/` and gets there via
> `git pull` from your dev machine's commits.

> **Two listener ports.** Ductile has two HTTP listeners by design — `8888`
> is the authenticated **control API** (Bearer-token, scoped), `8091` is the
> **webhook ingress** (HMAC-signed, body-as-trigger). The verify step uses
> 8888; the trigger uses 8091. They are separate listeners, not a typo.

There are five things to put in place. Order matters because ductile validates
checksums of the high-security files at startup.

### 1. Pick a strong secret (32 random hex bytes is fine)

```bash
SECRET=$(openssl rand -hex 32)
echo "$SECRET"   # save this — you'll need it on both ends
```

### 2. Make the secret available inside the ductile container

Add the line to `/mnt/user/appdata/ductile/.env` (docker-compose auto-reads
it; the file may not exist yet — create it):

```
MATTJOYCE_PUBLISH_SECRET=<SECRET-from-step-1>
```

And reference it from the ductile `docker-compose.yml` `environment:` block:

```yaml
environment:
  - MATTJOYCE_PUBLISH_SECRET=${MATTJOYCE_PUBLISH_SECRET:-}
```

> **Gotcha:** `.env` is only re-read on `docker compose up` — NOT on
> `docker restart`. After changing the secret, step 5 uses `docker compose
> up -d` for that reason.

### 3. Add the three config entries

`/mnt/user/appdata/ductile/config/tokens.yaml`:

```yaml
tokens:
  - name: mattjoyce_publish_secret
    key: ${MATTJOYCE_PUBLISH_SECRET}
```

`/mnt/user/appdata/ductile/config/webhooks.yaml` — append to the existing
`endpoints:` list (do NOT replace it, other webhooks already live there):

```yaml
- name: mattjoyce_publish
  path: /webhook/mattjoyce-publish
  plugin: astro_rebuild_prod
  secret_ref: mattjoyce_publish_secret
  signature_header: X-Ductile-Signature-256
```

`/mnt/user/appdata/ductile/config/plugins.yaml`:

```yaml
astro_rebuild_prod:
  enabled: true
  parallelism: 1            # serialize concurrent webhook fires
  timeout: 300s
  max_attempts: 1           # don't retry — the webhook caller can re-fire
  config:
    command: "docker compose build && docker compose up -d"
    working_dir: /mnt/user/appdata/matt_joyce
    event_type: mattjoyce.publish.completed
    emit_event: true
    timeout_seconds: 300
    stdout_max_bytes: 4096
    stderr_max_bytes: 4096
```

### 4. Bind-mount the compose project (read-only)

In ductile's `docker-compose.yml`, under `volumes:`:

```yaml
- /mnt/user/appdata/matt_joyce:/mnt/user/appdata/matt_joyce:ro
```

### 5. Refresh checksums and bring up

There are three small operations here:

```bash
# (a) Rebuild the ductile image so the new plugin folder gets baked in.
#     The Dockerfile uses `COPY --from=plugins-extra .` to pull
#     /mnt/user/Projects/ductile-plugins/ at build time — make sure your
#     new plugin folder is already on the Unraid filesystem there (via
#     `git pull` on Unraid), otherwise the bake will skip it silently.
cd /mnt/user/appdata/ductile && docker compose up --build -d

# (b) Refresh the config lock — tokens.yaml + webhooks.yaml are high-security
#     and refuse to load on checksum mismatch.  This needs a one-shot
#     CONTAINER WITH `:rw` because the live container mounts /app/config
#     read-only.
docker run --rm \
  -v /mnt/user/appdata/ductile/config:/app/config:rw \
  ductile-ductile:latest \
  /app/ductile config lock --config-dir /app/config

# (c) Apply the new env + restart with the new image.
cd /mnt/user/appdata/ductile && docker compose up -d

# (d) Verify the new plugin loaded.
curl -s -H "Authorization: Bearer <YOUR_API_TOKEN>" \
  http://<unraid>:8888/plugins | jq '.[] | select(.name=="astro_rebuild_prod")'
```

> If you ALSO needed to enable the webhook listener for the first time
> (no other webhooks in `webhooks.yaml`), make sure `webhooks.yaml` is in
> the `include:` list of `config.yaml`, AND that `config.yaml` has a
> `webhooks: { listen: "0.0.0.0:8091" }` block. Missing either of those is
> a common first-time trap — see "Failure modes" below.

---

## How to trigger it

From any machine with a copy of the secret, the body of a `POST` is signed
with HMAC-SHA256 and sent to `/webhook/mattjoyce-publish`:

```bash
SECRET=$(cat ~/.config/secrets/ductile/.env | grep MATTJOYCE_PUBLISH_SECRET | cut -d= -f2-)
BODY='{"trigger":"manual","actor":"matt@mac"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $2}')

curl -fsS -X POST http://192.168.20.4:8091/webhook/mattjoyce-publish \
  -H "Content-Type: application/json" \
  -H "X-Ductile-Signature-256: sha256=$SIG" \
  -d "$BODY"
```

A successful trigger returns JSON like `{"job_id":"job-..."}`. The actual
rebuild proceeds asynchronously inside ductile.

The body content is not interpreted by the plugin — it's only used as the
*input to the HMAC*. Pass an empty string `''` if you prefer; the signature
must match whatever you send.

---

## How to verify it worked

Right after triggering:

```bash
# 1. job_id returned — confirms the webhook signature checked and a job enqueued
# 2. tail ductile job log for this plugin
docker exec ductile /app/ductile job logs --plugin astro_rebuild_prod --limit 1 --json

# 3. after ~30-60s (depends on Astro build time), confirm prod is serving fresh content
curl -sI https://mattjoyce.ai/ | head -3
```

The job log entry will include `payload.exit_code`. `0` means success;
anything else means the rebuild failed and you should pull `payload.stderr`
to see what broke.

---

## Failure modes (and what to do about each)

| Symptom | Likely cause | Fix |
|---|---|---|
| Webhook returns 401/403 | Signature mismatch | Verify `SECRET` matches between Mac `.env` and ductile container env. The variable expansion `${MATTJOYCE_PUBLISH_SECRET}` in `tokens.yaml` resolves at config load — if env is missing in the container, the secret is the **empty string** (silent and dangerous). Use `docker exec ductile env \| grep MATTJOYCE`. |
| Webhook returns 404 | New endpoint not loaded | You probably skipped the `docker restart ductile` step. Webhooks are loaded on startup. |
| Webhook returns 5xx | `webhooks.yaml` or `tokens.yaml` failed checksum validation | Run `config lock` (step 5 above). High-security files refuse to load on mismatch. |
| Job enqueued but exits non-zero with "no such file or directory" | `working_dir` not visible inside the ductile container | Check the bind mount (step 4 above). `docker exec ductile ls /mnt/user/appdata/matt_joyce` should show the compose file. |
| Job exits with "Cannot connect to the Docker daemon" | Docker socket mount missing/wrong | Verify `/var/run/docker.sock:/var/run/docker.sock` is mounted in ductile's compose. |
| Job hangs and is killed at 300s | Build genuinely takes longer than the timeout | Bump `timeout_seconds` and `timeout` in plugins.yaml. Astro builds the whole site every time; if it ever exceeds 5min, that's a content-volume signal. |
| Two webhooks fire within seconds, second job fails or produces a corrupt build | Concurrent docker compose runs racing | `parallelism: 1` in plugins.yaml (above) serializes. Verify it's set; the schema default is also 1 but be explicit. |
| `config lock` fails with "read-only file system" | You ran it inside the live ductile container (`/app/config` is mounted `:ro` there). | Use the one-shot rw container form shown in step 5(b) above. This is the single most common first-time trap on this stack. |
| Webhook listener not reachable at all (connection refused on 8091) | Webhook listener wasn't enabled in `config.yaml`, OR `webhooks.yaml` is missing from `config.yaml` `include:` list. | Add a `webhooks: { listen: "0.0.0.0:8091" }` block to `config.yaml`, AND add `webhooks.yaml` to its `include:` list, then refresh lock + bring up. |
| `Cannot connect to docker daemon` inside the plugin job (`exit_code` 1, stderr says socket missing) | `/var/run/docker.sock` mount is missing from ductile's `docker-compose.yml`. | Add `- /var/run/docker.sock:/var/run/docker.sock` under `volumes:`, then `docker compose up -d`. |

---

## Security model

This plugin trusts exactly one thing: that the HMAC over the request body was
computed with the configured shared secret. Everything else is operator-controlled:

- **The command is in `plugins.yaml`, not the webhook payload.** A successful
  signature verification only authorizes "run the operator's configured command,
  unchanged." It does NOT allow the caller to influence what command runs.
  Payload values are passed to the spawned process only as `DUCTILE_PAYLOAD_*`
  environment variables (`payload_value_to_env`), never spliced into the
  command string.
- **The plugin has docker.sock access via the ductile container's mount.**
  That's root-equivalent on the Unraid host. The privilege boundary is
  therefore the webhook signature, not the plugin. Rotate the secret if
  there's any reason to believe it leaked — and rebuild the ductile image
  to expire any in-flight requests.
- **The webhook listener is LAN-only by default** (`0.0.0.0:8091`, only
  reachable inside the Unraid LAN). If you tunnel it to the internet (e.g.
  via Cloudflare), the signature is now the *only* thing standing between a
  random POST and a production rebuild — and you should think hard about
  rate-limiting and rotation cadence at that point.

---

## Design notes (Hickey, Armstrong)

This plugin exists because the prior deploy chain
(`sshpass -p PASSWORD ssh root@host docker compose build && up -d`) coupled
*the deploy* to *one specific operator on one specific laptop with one
specific shell environment knowing one specific password*. That's
complecting the trigger, the auth, the transport, and the action. The
webhook design separates them:

- **Trigger** is "an HTTP request arrived." (Could be `curl`, could be a
  GitHub action, could be a button in a UI.)
- **Auth** is "the body was signed with the shared secret." (Independent of
  who sent it or from where.)
- **Transport** is "HTTP over the LAN to port 8091." (Replaceable.)
- **Action** is "run the operator-configured shell command." (Defined in
  `plugins.yaml`, version-controlled separately from any of the above.)

Each piece can change without dragging the others along. That's *simple* in
Hickey's sense — not necessarily *easy* (the wiring is fiddlier than the
original sshpass one-liner) but much more *composable*.

The Armstrong influence is in what this plugin **doesn't** try to do:
- It doesn't catch and rescue errors. `docker compose` returns non-zero;
  the plugin reports that. The job runner records the failure. Operators
  see it. *Let it crash.*
- It doesn't share state with `astro_rebuild_staging`. Different host,
  different docker daemon, different config. A staging outage cannot
  wedge prod. *Isolate.*
- It doesn't try to be idempotent across calls. Fire it twice and you get
  two rebuilds (serialized by `parallelism: 1`). Idempotency belongs at
  the caller (use a dedupe key in the payload if you care) or at a higher
  layer (a pipeline that drops duplicates within a window). *Don't paper
  over what the caller's contract should specify.*

---

## Future decompletion

The `astro_rebuild_staging` and `astro_rebuild_prod` plugins are folder-level
clones of the same `run.py`. This is the simplest pattern but not the
simplest model. The cleaner Hickey shape is:

- One canonical generic plugin called something like `shell_exec` (in core
  ductile) or kept as-is here.
- Multiple `plugins.yaml` entries that alias it via the `uses:` field — see
  the `ap_canary` precedent in the canonical Unraid deploy runbook, where
  `ap_canary: uses: agent_handshake` keeps a single plugin implementation
  and many operator-facing names.

```yaml
# Future shape — not what's wired today
mattjoyce_publish:
  uses: shell_exec
  config:
    command: "docker compose build && docker compose up -d"
    working_dir: /mnt/user/appdata/matt_joyce
    event_type: mattjoyce.publish.completed
```

When more callers start needing the same shape, that's the refactor.
Today, two clones is a small, honest amount of duplication and the
operator-facing log lines (`plugin=astro_rebuild_prod`) are unambiguous.

---

## See also

- Canonical deploy runbook: `unraid_admin/vault/Ductile Integration Gateway.md`
- Source plugin: `ductile-plugins/astro_rebuild_staging/`
- Webhook docs: `ductile/docs/WEBHOOKS.md`
- Site repo: `~/Projects/matt_joyce/` (compose project at `/mnt/user/appdata/matt_joyce/` on Unraid)
