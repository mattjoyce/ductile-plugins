# ductile-plugins

Community and integration plugins for the [Ductile Integration Gateway](https://github.com/mattjoyce/ductile).

## What's here

Each directory is a self-contained plugin implementing the Ductile plugin protocol (v2).

| Plugin | Description |
|--------|-------------|
| `astro_rebuild_staging` | Trigger an Astro staging site rebuild |
| `changelog_microblog` | Post changelog entries to a microblog |
| `check_youtube` | Check YouTube channel for new videos |
| `discord_notify` | Send notifications to a Discord channel |
| `fabric` | Run prompts through the Fabric CLI |
| `git_commit_push` | Commit and push changes to a git repo |
| `git_repo_sync` | Sync a local git repo with its remote |
| `github_repo_sync` | Sync a GitHub repo |
| `jina-reader` | Fetch clean article text via Jina Reader |
| `repo_policy` | Enforce policy rules on a git repository |
| `youtube_playlist` | Fetch a YouTube playlist |
| `youtube_transcript` | Fetch a YouTube video transcript |

## Plugin structure

Each plugin contains:
- `manifest.yaml` — plugin metadata and command definitions (see `plugin-manifest.schema.json`)
- `run.py` / `run.sh` / `run.ts` — entrypoint script
- `README.md` — usage and configuration

## Using a plugin

Point your ductile config at the plugin directory:

```yaml
plugins:
  fabric:
    path: /path/to/ductile-plugins/fabric
    enabled: true
    config:
      # plugin-specific config keys
```

See each plugin's README for available config keys.

## License

Apache 2.0 — see [LICENSE](LICENSE).
