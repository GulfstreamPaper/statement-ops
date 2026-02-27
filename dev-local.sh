#!/usr/bin/env bash
set -euo pipefail
cd "/Users/ivan/Documents/New project"
source .venv/bin/activate
export DATA_DIR="/tmp/statement-ops-local"
export APP_USERNAME="local"
export APP_PASSWORD="localpass"
export SECRET_KEY="local-dev-secret"
python3 app.py
