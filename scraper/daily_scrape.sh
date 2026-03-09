#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
#  HEB Price Tracker — Daily Scrape + Supabase Sync
#
#  Schedule: runs at 7am via launchd.
#  - If Mac was asleep at 7am, waits 15 minutes after wake before running.
#  - Only runs once per day (lock file prevents double-runs).
#  - Logs everything to scraper/scraper.log
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="$PROJECT_DIR/venv/bin/python3"
LOG="$PROJECT_DIR/scraper/scraper.log"
LOCK_DIR="/tmp/heb_tracker_locks"
LOCK_FILE="$LOCK_DIR/ran_$(date +%Y-%m-%d).lock"

mkdir -p "$LOCK_DIR"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S')  $1" | tee -a "$LOG"
}

# ── Once-per-day guard ────────────────────────────────────────────────────────
if [ -f "$LOCK_FILE" ]; then
    log "Already ran today — skipping."
    exit 0
fi

# ── 15-minute wake delay ──────────────────────────────────────────────────────
# If the current time is more than 10 minutes past 7:00am, the Mac was likely
# asleep at the scheduled time. Wait 15 minutes to let the system settle.
SCHEDULED_HOUR=7
CURRENT_HOUR=$(date +%H)
CURRENT_MIN=$(date +%M)
CURRENT_TOTAL_MINS=$(( 10#$CURRENT_HOUR * 60 + 10#$CURRENT_MIN ))
SCHEDULED_TOTAL_MINS=$(( SCHEDULED_HOUR * 60 + 10 ))   # 7:10am grace window

if [ "$CURRENT_TOTAL_MINS" -gt "$SCHEDULED_TOTAL_MINS" ]; then
    log "Mac woke after scheduled time — waiting 15 minutes before scraping..."
    sleep 900
fi

# ── Mark as running ───────────────────────────────────────────────────────────
touch "$LOCK_FILE"

# ── Run scraper ───────────────────────────────────────────────────────────────
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "Starting daily HEB scrape..."

cd "$PROJECT_DIR/scraper" || exit 1
"$PYTHON" full_scraper2.py >> "$LOG" 2>&1
SCRAPER_EXIT=$?

if [ "$SCRAPER_EXIT" -eq 0 ]; then
    log "Scraper finished successfully."
else
    log "Scraper exited with code $SCRAPER_EXIT — check log above."
fi

# ── Sync to Supabase ──────────────────────────────────────────────────────────
log "Syncing new records to Supabase..."

cd "$PROJECT_DIR/supabase" || exit 1
"$PYTHON" sync_to_supabase.py >> "$LOG" 2>&1
SYNC_EXIT=$?

if [ "$SYNC_EXIT" -eq 0 ]; then
    log "Supabase sync complete."
else
    log "Supabase sync exited with code $SYNC_EXIT — check log above."
fi

log "Daily job done."
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
