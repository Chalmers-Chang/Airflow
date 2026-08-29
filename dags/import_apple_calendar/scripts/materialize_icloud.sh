#!/bin/bash
# Run on the Mac host. Docker cannot ask iCloud to download cloud-only files.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_DIR="$REPO_ROOT/airflow.deployment/docker-compose"
CONTAINER_ROOT="/opt/airflow/icloud"
LABEL="com.chalmers.airflow.materialize-icloud"
AGENT_PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"

if [ -z "${ICLOUD_AIRFLOW_DIR:-}" ] && [ -f "$COMPOSE_DIR/.env" ]; then
  ICLOUD_AIRFLOW_DIR="$(grep '^ICLOUD_AIRFLOW_DIR=' "$COMPOSE_DIR/.env" | cut -d= -f2-)"
fi
HOST_ROOT="${ICLOUD_AIRFLOW_DIR:-$HOME/Library/Mobile Documents/com~apple~CloudDocs/airflow}"

install_agent() {
  mkdir -p "${HOME}/Library/LaunchAgents"
  cat >"$AGENT_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${SCRIPT_DIR}/materialize_icloud.sh</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin</string>
    <key>ICLOUD_AIRFLOW_DIR</key>
    <string>${HOST_ROOT}</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>120</integer>
  <key>StandardOutPath</key>
  <string>${REPO_ROOT}/dags/logs/import_apple_calendar/materialize_icloud.log</string>
  <key>StandardErrorPath</key>
  <string>${REPO_ROOT}/dags/logs/import_apple_calendar/materialize_icloud.log</string>
</dict>
</plist>
EOF
  mkdir -p "$REPO_ROOT/dags/logs/import_apple_calendar"
  launchctl bootout "gui/$(id -u)" "$AGENT_PLIST" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$AGENT_PLIST"
  echo "installed $AGENT_PLIST (every 120s)"
}

readable_in_docker() {
  local cid="$1" cpath="$2"
  docker exec "$cid" sha256sum "$cpath" >/dev/null 2>&1
}

materialize() {
  local hpath="$1"
  brctl download "$hpath" 2>/dev/null || true
  if [ ! -e "$hpath" ] || [[ "$hpath" == *.icloud ]]; then
    open -g "$hpath" 2>/dev/null || true
    return
  fi
}

run() {
  brctl download "$HOST_ROOT" 2>/dev/null || true

  local cid=""
  if [ -f "$COMPOSE_DIR/docker-compose.yaml" ]; then
    cid="$(docker compose -f "$COMPOSE_DIR/docker-compose.yaml" --project-directory "$COMPOSE_DIR" ps -q airflow-worker 2>/dev/null || true)"
  fi
  if [ -z "$cid" ]; then
    echo "airflow-worker not running; asked iCloud to download $HOST_ROOT"
    return 0
  fi

  local cpath rel hpath i
  while IFS= read -r cpath; do
    [ -n "$cpath" ] || continue
    rel="${cpath#"$CONTAINER_ROOT"/}"
    hpath="$HOST_ROOT/$rel"
    if [[ "$cpath" != *.icloud ]] && readable_in_docker "$cid" "$cpath"; then
      continue
    fi
    echo "downloading $rel"
    materialize "$hpath"
    if [[ "$cpath" == *.icloud ]]; then
      hpath="${hpath%.icloud}"
      cpath="${cpath%.icloud}"
      materialize "$hpath"
    fi
    for i in 1 2 3 4 5 6; do
      if readable_in_docker "$cid" "$cpath"; then
        echo "ready $rel"
        break
      fi
      sleep 5
    done
    if ! readable_in_docker "$cid" "$cpath"; then
      open -g "$hpath" 2>/dev/null || true
      echo "still cloud-only (retry next run): $rel"
    fi
  done < <(docker exec "$cid" find "$CONTAINER_ROOT" \( -name '*.pdf' -o -name '*.icloud' \) -print)
}

if [ "${1:-}" = "install" ]; then
  install_agent
  run
else
  run
fi
