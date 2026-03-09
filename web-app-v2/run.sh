#!/bin/bash
# ─────────────────────────────────────────────────────────────
#  HEB Price Tracker v2 - Launcher
#  Installs Flask if needed, then starts the app
# ─────────────────────────────────────────────────────────────

# Use the project venv Python
PYTHON="/Users/memorodriguez/git/heb-grocery-tracker/venv/bin/python3"
PIP="/Users/memorodriguez/git/heb-grocery-tracker/venv/bin/pip"

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
