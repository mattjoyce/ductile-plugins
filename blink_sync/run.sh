#!/usr/bin/env bash
# blink_sync plugin — protocol v2
# Spawns the blink-sync Docker container via the host Docker socket.
# Config keys: data_dir, media_dir, delay, image
set -euo pipefail

request="$(cat)"

cmd=$(printf '%s' "$request" | jq -r '.command // "poll"')
data_dir=$(printf '%s' "$request" | jq -r '.config.data_dir // "/mnt/user/appdata/ductile/data/blink"')
media_dir=$(printf '%s' "$request" | jq -r '.config.media_dir // "/mnt/user/blink/media"')
delay=$(printf '%s' "$request" | jq -r '.config.delay // "1"')
image=$(printf '%s' "$request" | jq -r '.config.image // "blink-sync"')

ok_response() {
  local result="$1"
  jq -n --arg r "$result" \
    '{status:"ok", result:$r, logs:[{level:"info", message:$r}]}'
}

err_response() {
  local msg="$1"
  jq -n --arg e "$msg" \
    '{status:"error", error:$e, retry:false, logs:[{level:"error", message:$e}]}'
}

case "$cmd" in
  health)
    if docker image inspect "$image" >/dev/null 2>&1; then
      ok_response "blink-sync image '${image}' is present"
    else
      err_response "blink-sync image '${image}' not found — build it on the host first"
    fi
    ;;

  poll)
    cname="blink-sync-$$"
    trap 'docker stop "$cname" 2>/dev/null; exit 1' TERM INT

    output=$(docker run --rm --name "$cname" \
      -v "${data_dir}:/data" \
      -v "${media_dir}:/media" \
      -e CREDFILE=/data/blink.json \
      -e SAVEDIR=/media \
      -e STATEFILE=/data/downloaded_clips.json \
      -e DELAY="$delay" \
      "$image" 2>&1) && rc=0 || rc=$?

    trap - TERM INT

    if [ "$rc" -eq 0 ]; then
      ok_response "$output"
    else
      err_response "$output"
    fi
    ;;

  *)
    err_response "unknown command: ${cmd}"
    ;;
esac
