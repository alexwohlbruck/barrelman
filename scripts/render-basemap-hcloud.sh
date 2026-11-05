#!/usr/bin/env bash
# =============================================================================
# render-basemap-hcloud.sh — render basemap.pmtiles on a rented Hetzner Cloud
# server instead of on this host.
#
# Called by rebuild-basemap.sh when BASEMAP_RENDER=hcloud, in place of its local
# planetiler run. Everything around the render — the lock, the swap, the .prev
# copy, the martin restart — stays in rebuild-basemap.sh.
#
# Why: a US render to z15 holds tens of gigabytes of sort files and a node map
# it reads at random. On a box that also serves traffic it either gets the
# memory it wants and the kernel OOM-kills a service (MOTIS, 18 times in one
# z15 render), or it is capped and spends most of its time waiting on disk. A
# server rented for the two or three hours a render takes has the whole machine
# to itself, and costs cents.
#
# What it does:
#   1. deletes any render server a killed earlier run left behind
#   2. creates a throwaway SSH key and server (docker-ce image)
#   3. streams region.osm.pbf to it, runs planetiler there, streams the archive
#      back to <out>/<staging name>, and checks size and sha256 on both ends
#   4. deletes the server and the key, on success, failure or SIGTERM
#
# Expects from the caller (rebuild-basemap.sh exports these):
#   IN_DIR, OUT_DIR       host paths holding the extract and the archive
#   PBF_NAME, STAGING_NAME
#   BASEMAP_MAXZOOM, PLANETILER_IMAGE, HELPER_IMAGE
#
# Environment:
#   HCLOUD_TOKEN                 read/write API token. Give the render its own
#                                Hetzner project: a token can do anything in its
#                                project, including delete the production server.
#   BASEMAP_HCLOUD_SERVER_TYPES  comma list tried in order (default: cx53,cax41,ccx33)
#   BASEMAP_HCLOUD_LOCATIONS     comma list tried in order (default: nbg1,fsn1,hel1)
#   BASEMAP_HCLOUD_HEAP          planetiler heap on the rented server (default: 14g).
#                                The rest of its memory caches the node map.
#   BASEMAP_HCLOUD_IMAGE         server image (default: docker-ce)
# =============================================================================

HCLOUD_API="${HCLOUD_API:-https://api.hetzner.cloud/v1}"
HCLOUD_SERVER_TYPES="${BASEMAP_HCLOUD_SERVER_TYPES:-cx53,cax41,ccx33}"
HCLOUD_LOCATIONS="${BASEMAP_HCLOUD_LOCATIONS:-nbg1,fsn1,hel1}"
HCLOUD_HEAP="${BASEMAP_HCLOUD_HEAP:-14g}"
HCLOUD_IMAGE="${BASEMAP_HCLOUD_IMAGE:-docker-ce}"
# Every server and key this script creates carries this label, so a run can
# find what an earlier, killed run left behind — and nothing else. The value
# names this install, so two installs sharing a project never sweep each other's.
HCLOUD_LABEL_KEY="barrelman-basemap-render"

# Set once the server and key exist, so cleanup knows what to delete.
HCLOUD_SERVER_ID=""
HCLOUD_KEY_ID=""
HCLOUD_WORK=""
HCLOUD_PLACED=""

hlog() { echo "[$(date '+%H:%M:%S')] [basemap:hcloud] $*"; }

# A Hetzner label value: up to 63 of [A-Za-z0-9._-], starting and ending
# alphanumeric. Host names mostly qualify already; anything else is folded.
hcloud_label_value() {
  local v
  v="$(printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '-' | cut -c1-63)"
  v="$(printf '%s' "$v" | sed -E 's/^[^A-Za-z0-9]+//; s/[^A-Za-z0-9]+$//')"
  printf '%s' "${v:-barrelman}"
}

# One API call. Prints the response body; returns non-zero on an HTTP error,
# still printing the body, since Hetzner's error codes are in it.
hc_api() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -X "$method" -H "Authorization: Bearer ${HCLOUD_TOKEN}" -w '\n%{http_code}')
  [ -n "$body" ] && args+=(-H 'Content-Type: application/json' --data "$body")
  local out code
  out="$(curl "${args[@]}" "${HCLOUD_API}${path}")" || return 1
  code="${out##*$'\n'}"
  printf '%s' "${out%$'\n'*}"
  [ "${code:0:1}" = 2 ]
}

# json_get <expression> — evaluate a Python expression over the JSON on stdin,
# bound to `d`. Prints nothing for a missing key.
json_get() {
  python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    v = eval(sys.argv[1])
except Exception:
    sys.exit(0)
if v is None:
    sys.exit(0)
print(v if not isinstance(v, list) else "\n".join(str(x) for x in v))
' "$1"
}

# Delete every server and SSH key labelled for this install. Run before
# creating anything: the basemap lock means no other render of this install is
# alive, so whatever carries the label was orphaned by a run that was killed
# (SIGKILL skips the EXIT trap) and is still being billed.
hcloud_sweep() {
  local selector="${HCLOUD_LABEL_KEY}%3D${HCLOUD_LABEL_VALUE}" id
  for id in $(hc_api GET "/servers?label_selector=${selector}" | json_get '[s["id"] for s in d["servers"]]'); do
    hlog "deleting server ${id}, left behind by an earlier render"
    hc_api DELETE "/servers/${id}" >/dev/null || hlog "WARNING: could not delete leftover server ${id}"
  done
  for id in $(hc_api GET "/ssh_keys?label_selector=${selector}" | json_get '[k["id"] for k in d["ssh_keys"]]'); do
    hc_api DELETE "/ssh_keys/${id}" >/dev/null || true
  done
}

# hcloud_create_server <name> <ssh key id>
# Tries each server type in each location until one is accepted, since a type
# is regularly out of stock in one location and not another, and a project's
# core limits can rule a type out (shared and dedicated cores are counted
# separately). Sets
# HCLOUD_SERVER_ID and HCLOUD_PLACED ("<type> <location>"). Call it directly,
# not in $(…): a subshell would keep the id from cleanup.
hcloud_create_server() {
  local name="$1" key_id="$2" type loc resp code
  local IFS_SAVE="$IFS"
  IFS=','
  local types=($HCLOUD_SERVER_TYPES) locs=($HCLOUD_LOCATIONS)
  IFS="$IFS_SAVE"
  for type in "${types[@]}"; do
    for loc in "${locs[@]}"; do
      local body
      body="$(printf '{"name":"%s","server_type":"%s","location":"%s","image":"%s","ssh_keys":[%s],"labels":{"%s":"%s"},"start_after_create":true}' \
        "$name" "$type" "$loc" "$HCLOUD_IMAGE" "$key_id" "$HCLOUD_LABEL_KEY" "$HCLOUD_LABEL_VALUE")"
      if resp="$(hc_api POST /servers "$body")"; then
        HCLOUD_SERVER_ID="$(printf '%s' "$resp" | json_get 'd["server"]["id"]')"
        if [ -n "$HCLOUD_SERVER_ID" ]; then
          HCLOUD_PLACED="$type $loc"
          return 0
        fi
      fi
      code="$(printf '%s' "$resp" | json_get 'd["error"]["code"]')"
      case "$code" in
        resource_unavailable|resource_limit_exceeded|placement_error|server_type_unavailable|invalid_input|unsupported_location_for_server_type)
          hlog "${type} in ${loc}: ${code:-rejected}, trying the next" >&2 ;;
        *)
          hlog "ERROR: server create failed: ${resp}" >&2
          return 1 ;;
      esac
    done
  done
  hlog "ERROR: none of ${HCLOUD_SERVER_TYPES} is available in ${HCLOUD_LOCATIONS}" >&2
  return 1
}

# Delete what this run created. Safe to call twice and with nothing created.
hcloud_cleanup() {
  if [ -n "$HCLOUD_SERVER_ID" ]; then
    if hc_api DELETE "/servers/${HCLOUD_SERVER_ID}" >/dev/null; then
      hlog "deleted render server ${HCLOUD_SERVER_ID}"
    else
      hlog "WARNING: could not delete render server ${HCLOUD_SERVER_ID} — the next render deletes it, or remove it by hand" >&2
    fi
    HCLOUD_SERVER_ID=""
  fi
  if [ -n "$HCLOUD_KEY_ID" ]; then
    hc_api DELETE "/ssh_keys/${HCLOUD_KEY_ID}" >/dev/null || true
    HCLOUD_KEY_ID=""
  fi
  [ -n "$HCLOUD_WORK" ] && rm -rf "$HCLOUD_WORK"
  HCLOUD_WORK=""
}

remote() {
  ssh -i "${HCLOUD_WORK}/key" \
    -o UserKnownHostsFile="${HCLOUD_WORK}/known_hosts" -o StrictHostKeyChecking=accept-new \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -o ConnectTimeout=10 -o LogLevel=ERROR \
    "root@${HCLOUD_IP}" "$@"
}

# Run a shell command in a throwaway container holding the archive directories,
# the same way rebuild-basemap.sh reaches them (see its io_sh).
helper() {
  docker run --rm -i -v "${IN_DIR}:/in:ro" -v "${OUT_DIR}:/out" "$HELPER_IMAGE" sh -c "$1"
}

render_basemap_hcloud() {
  if [ -z "${HCLOUD_TOKEN:-}" ]; then
    hlog "ERROR: BASEMAP_RENDER=hcloud needs HCLOUD_TOKEN" >&2
    return 1
  fi
  local tool
  for tool in curl ssh ssh-keygen python3; do
    command -v "$tool" >/dev/null || { hlog "ERROR: ${tool} is not installed in this container" >&2; return 1; }
  done

  HCLOUD_LABEL_VALUE="$(hcloud_label_value "${BASEMAP_HCLOUD_LABEL:-$(docker info --format '{{.Name}}' 2>/dev/null)}")"
  trap 'hcloud_cleanup' EXIT
  trap 'hcloud_cleanup; exit 143' TERM INT

  hcloud_sweep

  HCLOUD_WORK="$(mktemp -d)"
  ssh-keygen -q -t ed25519 -N '' -f "${HCLOUD_WORK}/key"
  local stamp name resp
  stamp="$(date -u +%Y%m%d-%H%M%S)"
  name="barrelman-basemap-${stamp}"
  resp="$(hc_api POST /ssh_keys "$(printf '{"name":"%s","public_key":"%s","labels":{"%s":"%s"}}' \
    "$name" "$(cat "${HCLOUD_WORK}/key.pub")" "$HCLOUD_LABEL_KEY" "$HCLOUD_LABEL_VALUE")")" || {
    hlog "ERROR: could not register the SSH key: ${resp}" >&2
    return 1
  }
  HCLOUD_KEY_ID="$(printf '%s' "$resp" | json_get 'd["ssh_key"]["id"]')"

  hcloud_create_server "$name" "$HCLOUD_KEY_ID" || return 1
  hlog "created ${name} (${HCLOUD_PLACED}), id ${HCLOUD_SERVER_ID}"

  local i status
  for i in $(seq 1 60); do
    resp="$(hc_api GET "/servers/${HCLOUD_SERVER_ID}")" || true
    status="$(printf '%s' "$resp" | json_get 'd["server"]["status"]')"
    [ "$status" = running ] && break
    sleep 5
  done
  HCLOUD_IP="$(printf '%s' "$resp" | json_get 'd["server"]["public_net"]["ipv4"]["ip"]')"
  if [ "$status" != running ] || [ -z "$HCLOUD_IP" ]; then
    hlog "ERROR: server never reached running (status: ${status:-unknown})" >&2
    return 1
  fi
  # The docker-ce image starts docker from cloud-init, a little after sshd.
  for i in $(seq 1 60); do
    remote 'docker info >/dev/null 2>&1' 2>/dev/null && break
    sleep 5
  done
  if ! remote 'docker info >/dev/null 2>&1'; then
    hlog "ERROR: docker never came up on ${HCLOUD_IP}" >&2
    return 1
  fi

  local pbf_size
  pbf_size="$(helper "stat -c %s /in/${PBF_NAME}" | tr -cd '0-9')"
  hlog "sending ${PBF_NAME} ($((pbf_size / 1024 / 1024)) MB) to ${HCLOUD_IP}"
  helper "cat /in/${PBF_NAME}" | remote "mkdir -p /work && cat > /work/${PBF_NAME}"
  if [ "$(remote "stat -c %s /work/${PBF_NAME}" | tr -cd '0-9')" != "$pbf_size" ]; then
    hlog "ERROR: ${PBF_NAME} arrived truncated" >&2
    return 1
  fi

  # The water polygons and Natural Earth planetiler needs besides the extract,
  # about a gigabyte. A local render keeps them beside the archive, so send
  # those rather than fetch them again; --download still fills in anything
  # missing.
  if helper 'test -d /out/sources' 2>/dev/null; then
    hlog "sending planetiler's other sources"
    helper 'tar -C /out -cf - sources' | remote 'tar -C /work -xf -' || hlog "WARNING: could not send them; planetiler will download them"
  fi

  hlog "rendering to z${BASEMAP_MAXZOOM} (heap ${HCLOUD_HEAP})"
  # Detached, so a dropped SSH session loses only the log tail, not the render.
  # Host networking and IPv4: Docker's bridge has no IPv6 route, and Java picks
  # the AAAA record of osmdata.openstreetmap.de, so --download failed with "No
  # route to host".
  remote "docker run -d --name render --network host \
    -e JAVA_TOOL_OPTIONS='-Xmx${HCLOUD_HEAP} -Djava.net.preferIPv4Addresses=true' \
    -v /work:/work ${PLANETILER_IMAGE} \
    --osm-path=/work/${PBF_NAME} --output=/work/basemap.pmtiles \
    --download --download-dir=/work/sources --tmpdir=/work/tmp \
    --maxzoom=${BASEMAP_MAXZOOM} --force" >/dev/null || {
    hlog "ERROR: could not start planetiler on the render server" >&2
    return 1
  }
  local since=0
  while :; do
    remote "docker logs -f --since ${since} render 2>&1" || true
    since="$(date +%s)"
    [ "$(remote "docker inspect -f '{{.State.Running}}' render" 2>/dev/null)" = true ] || break
    hlog "log stream dropped; reattaching"
  done
  local exit_code
  exit_code="$(remote "docker inspect -f '{{.State.ExitCode}}' render" | tr -cd '0-9')"
  if [ "${exit_code:-1}" != 0 ]; then
    hlog "ERROR: planetiler exited ${exit_code:-unknown} on the render server" >&2
    return 1
  fi

  local size sum
  size="$(remote 'stat -c %s /work/basemap.pmtiles' | tr -cd '0-9')"
  sum="$(remote 'sha256sum /work/basemap.pmtiles' | cut -d' ' -f1)"
  hlog "fetching the archive ($((size / 1024 / 1024)) MB)"
  remote 'cat /work/basemap.pmtiles' | helper "cat > /out/${STAGING_NAME}"
  if [ "$(helper "sha256sum /out/${STAGING_NAME}" | cut -d' ' -f1)" != "$sum" ]; then
    hlog "ERROR: the archive changed in transit (sha256 mismatch)" >&2
    helper "rm -f /out/${STAGING_NAME}" || true
    return 1
  fi
  hlog "archive received and verified"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  set -uo pipefail
  render_basemap_hcloud
fi
