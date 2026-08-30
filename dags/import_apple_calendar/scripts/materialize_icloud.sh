#!/bin/bash
# Mac host only. One-shot: download unread iCloud PDFs, wait 10s × 18, then exit.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_DIR="$REPO_ROOT/airflow.deployment/docker-compose"
CONTAINER_ROOT="/opt/airflow/icloud"
LABEL="com.chalmers.airflow.materialize-icloud"
AGENT_PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
WAIT_SEC=10
WAIT_TIMES=18
# SSH BatchMode sessions get a minimal PATH (/usr/bin:/bin:...); Docker Desktop
# installs to /usr/local/bin or /opt/homebrew/bin.
export PATH="/usr/local/bin:/opt/homebrew/bin:${PATH:-/usr/bin:/bin:/usr/sbin:/sbin}"

if [ -z "${ICLOUD_AIRFLOW_DIR:-}" ] && [ -f "$COMPOSE_DIR/.env" ]; then
  ICLOUD_AIRFLOW_DIR="$(grep '^ICLOUD_AIRFLOW_DIR=' "$COMPOSE_DIR/.env" | cut -d= -f2-)"
fi
HOST_ROOT="${ICLOUD_AIRFLOW_DIR:-$HOME/Library/Mobile Documents/com~apple~CloudDocs/airflow}"

uninstall_agent() {
  launchctl bootout "gui/$(id -u)" "$AGENT_PLIST" 2>/dev/null || true
  launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
  rm -f "$AGENT_PLIST"
  echo "removed LaunchAgent $LABEL"
}

host_path() {
  local p="$1"
  case "$p" in
    "$CONTAINER_ROOT"/*) echo "$HOST_ROOT/${p#"$CONTAINER_ROOT"/}" ;;
    /*) echo "$p" ;;
    *) echo "$HOST_ROOT/$p" ;;
  esac
}

container_path() {
  local p="$1"
  case "$p" in
    "$CONTAINER_ROOT"/*) echo "$p" ;;
    "$HOST_ROOT"/*) echo "$CONTAINER_ROOT/${p#"$HOST_ROOT"/}" ;;
    *) echo "$CONTAINER_ROOT/$p" ;;
  esac
}

worker_cid() {
  # Multiple replicas → take one container for readability checks.
  docker compose -f "$COMPOSE_DIR/docker-compose.yaml" --project-directory "$COMPOSE_DIR" ps -q airflow-worker 2>/dev/null | head -n 1 || true
}

readable_in_docker() {
  local cid="$1" cpath="$2"
  [ -n "$cid" ] && docker exec "$cid" sha256sum "$cpath" >/dev/null 2>&1
}

materialize() {
  local hpath="$1"
  brctl download "$hpath" 2>/dev/null || true
  open -g "$hpath" 2>/dev/null || true
}

collect_targets() {
  local cid="$1"
  if [ "$#" -gt 1 ]; then
    shift
    printf '%s\n' "$@"
    return
  fi
  docker exec "$cid" find "$CONTAINER_ROOT" \( -name '*.pdf' -o -name '*.icloud' \) -print
}

run() {
  local cid
  cid="$(worker_cid)"
  if [ -z "$cid" ]; then
    echo "airflow-worker not running"
    return 1
  fi

  local -a targets=()
  local line
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    targets+=("$line")
  done < <(collect_targets "$cid" "$@")

  if [ "${#targets[@]}" -eq 0 ]; then
    echo "no pdf targets"
    return 0
  fi

  brctl download "$HOST_ROOT" 2>/dev/null || true

  local n cpath hpath pending
  for n in $(seq 1 "$WAIT_TIMES"); do
    pending=0
    for cpath in "${targets[@]}"; do
      cpath="$(container_path "$cpath")"
      if [[ "$cpath" == *.icloud ]]; then
        hpath="$(host_path "$cpath")"
        materialize "$hpath"
        materialize "${hpath%.icloud}"
        cpath="${cpath%.icloud}"
      fi
      if readable_in_docker "$cid" "$cpath"; then
        continue
      fi
      pending=1
      hpath="$(host_path "$cpath")"
      echo "downloading ${cpath#"$CONTAINER_ROOT"/} (try $n/$WAIT_TIMES)"
      materialize "$hpath"
    done
    if [ "$pending" -eq 0 ]; then
      echo "ready"
      return 0
    fi
    if [ "$n" -lt "$WAIT_TIMES" ]; then
      sleep "$WAIT_SEC"
    fi
  done
  echo "still cloud-only after ${WAIT_TIMES}×${WAIT_SEC}s"
  return 1
}

if [ "${1:-}" = "uninstall" ]; then
  uninstall_agent
else
  run "$@"
fi
