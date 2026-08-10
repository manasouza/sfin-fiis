#!/usr/bin/env bash
set -euo pipefail

: "${SPREADSHEET_ID:?SPREADSHEET_ID must be set}"
: "${CREDENTIALS_PATH:?CREDENTIALS_PATH must be set}"

PYENV_DIR="${PYENV_DIR:-$HOME/pyenvs/sfin-fiis}"
APP_DIR="${APP_DIR:-$HOME/sfin-fiis}"
LOG_FILE="${LOG_FILE:-/var/log/sfinfiis-$(date +%Y%m%d).log}"

if [[ "$CREDENTIALS_PATH" == "~/"* ]]; then
  CREDENTIALS_PATH="$HOME/${CREDENTIALS_PATH#~/}"
  export CREDENTIALS_PATH
fi

cd "$APP_DIR"
source "$PYENV_DIR/bin/activate"

{
  echo ""
  echo "##########################################################################################################"
  echo "Today is $(date)"
  echo "##########################################################################################################"
  python main_local.py "$@"
  echo "##########################################################################################################"
  echo "Execution finished at $(date)"
  echo "##########################################################################################################"
} >> "$LOG_FILE" 2>&1
