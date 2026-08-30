#!/bin/bash
# Mac host: bootstrap AIRFLOW_DATA_DIR for import_apple_calendar (config, state, SSH key).
# Persistent outside the git repo so `git pull` does not wipe local setup.
# Inside containers this directory is always mounted at /opt/data/airflow.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_DIR="$REPO_ROOT/airflow.deployment/docker-compose"
EXAMPLE="$REPO_ROOT/dags/import_apple_calendar/config/source_config.json.example"
DEFAULT_HOST_DATA="${HOME}/Library/Application Support/airflow"

if [ -z "${AIRFLOW_DATA_DIR:-}" ] && [ -f "$COMPOSE_DIR/.env" ]; then
  AIRFLOW_DATA_DIR="$(grep '^AIRFLOW_DATA_DIR=' "$COMPOSE_DIR/.env" | cut -d= -f2- || true)"
fi
DATA_ROOT="${AIRFLOW_DATA_DIR:-$DEFAULT_HOST_DATA}"
DATA_DIR="$DATA_ROOT/import_apple_calendar"
KEY_DIR="$DATA_DIR/ssh"
KEY="$KEY_DIR/id_ed25519"
PUB="$KEY.pub"
AUTH="${HOME}/.ssh/authorized_keys"
COMMENT="airflow-import-apple-calendar"
CONFIG="$DATA_DIR/source_config.json"

mkdir -p "$KEY_DIR" "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh" 2>/dev/null || true

if [ ! -f "$CONFIG" ]; then
  if [ -f "$EXAMPLE" ]; then
    cp "$EXAMPLE" "$CONFIG"
    echo "created $CONFIG from example — edit Apple ID email / calendar_name"
  else
    echo "missing example at $EXAMPLE" >&2
    exit 1
  fi
else
  echo "config already present: $CONFIG"
fi

if [ ! -f "$KEY" ]; then
  ssh-keygen -t ed25519 -f "$KEY" -N "" -C "$COMMENT"
fi
chmod 600 "$KEY"
touch "$AUTH"
chmod 600 "$AUTH"
PUB_LINE="$(cat "$PUB")"
if grep -qF "$PUB_LINE" "$AUTH"; then
  echo "authorized_keys already has this public key"
else
  if grep -qF "$COMMENT" "$AUTH"; then
    grep -vF "$COMMENT" "$AUTH" >"${AUTH}.tmp" || true
    mv "${AUTH}.tmp" "$AUTH"
    chmod 600 "$AUTH"
  fi
  echo "$PUB_LINE" >>"$AUTH"
  echo "appended $PUB to $AUTH"
fi

SCRIPT_ABS="$(cd "$SCRIPT_DIR" && pwd)/materialize_icloud.sh"
echo "host data dir: $DATA_DIR"
echo "container path: /opt/data/airflow/import_apple_calendar"
echo "key: $KEY"
echo "enable Remote Login: System Settings → General → Sharing → Remote Login"
echo "then set in airflow.deployment/docker-compose/.env:"
echo "  AIRFLOW_DATA_DIR=$DATA_ROOT"
echo "  IMPORT_APPLE_CALENDAR_SSH_USER=$(whoami)"
echo "  IMPORT_APPLE_CALENDAR_MATERIALIZE_SCRIPT=$SCRIPT_ABS"
echo "and recreate containers: cd airflow.deployment/docker-compose && docker compose up -d"
