#!/bin/bash
# Mac host: create a key the Airflow worker uses after Remote Login is on.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
KEY_DIR="$REPO_ROOT/dags/logs/import_apple_calendar/ssh"
KEY="$KEY_DIR/id_ed25519"
PUB="$KEY.pub"
AUTH="${HOME}/.ssh/authorized_keys"
COMMENT="airflow-import-apple-calendar"

mkdir -p "$KEY_DIR" "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"
if [ ! -f "$KEY" ]; then
  ssh-keygen -t ed25519 -f "$KEY" -N "" -C "$COMMENT"
fi
chmod 600 "$KEY"
touch "$AUTH"
chmod 600 "$AUTH"
if ! grep -qF "$COMMENT" "$AUTH"; then
  cat "$PUB" >>"$AUTH"
  echo "appended $PUB to $AUTH"
else
  echo "authorized_keys already has $COMMENT"
fi
echo "key: $KEY"
echo "enable Remote Login: System Settings → General → Sharing → Remote Login"
