#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec /opt/homebrew/bin/python3.12 "$SCRIPT_DIR/convert_raw.py" "$@"
