#!/usr/bin/env bash
set -euo pipefail

: "${SPREADSHEET_ID:?SPREADSHEET_ID must be set}"
: "${CREDENTIALS_PATH:?CREDENTIALS_PATH must be set}"

PYENV_DIR="${PYENV_DIR:-$HOME/pyenvs/sfin-fiis}"
APP_DIR="${APP_DIR:-$HOME/sfin-fiis}"
# Allow overriding target log directory; default to /var/log
LOG_DIR="${LOG_DIR:-/var/log}"
LOG_FILE_NAME="sfinfiis-$(date +%Y%m%d).log"

# Ensure log directory is usable; fallback to application dir if not writable
if mkdir -p "$LOG_DIR" 2>/dev/null && touch "$LOG_DIR/.sfinfiis_check" 2>/dev/null; then
  LOG_FILE="$LOG_DIR/$LOG_FILE_NAME"
  rm -f "$LOG_DIR/.sfinfiis_check" 2>/dev/null || true
else
  LOG_FILE="${LOG_FILE:-$APP_DIR/$LOG_FILE_NAME}"
fi

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
} 2>&1 | tee -a "$LOG_FILE"
