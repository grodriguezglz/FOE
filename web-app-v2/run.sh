#!/bin/bash
# ─────────────────────────────────────────────────────────────
#  HEB Price Tracker v2 - Launcher
#  Installs Flask if needed, then starts the app
# ─────────────────────────────────────────────────────────────

# Resolve venv relative to this script's location (web-app-v2/../venv)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON="$PROJECT_ROOT/venv/bin/python3"
PIP="$PROJECT_ROOT/venv/bin/pip"

echo ""
echo "═══════════════════════════════════════"
echo "  HEB Price Tracker v2"
echo "═══════════════════════════════════════"

# Check Flask is installed
if ! $PYTHON -c "import flask" 2>/dev/null; then
    echo ""
    echo "  Installing Flask and psycopg2..."
    $PIP install flask psycopg2-binary
    echo "  Done."
fi

echo ""
echo "  Starting server..."
echo "  Open: http://localhost:8080"
echo ""

# Run the app from the web-app-v2 directory
cd "$(dirname "$0")"
$PYTHON app.py
