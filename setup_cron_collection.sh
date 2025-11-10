#!/bin/bash
# Setup cron job for intraday Greeks collection
# Run this once to set up automatic data collection during market hours

PROJECT_DIR="/Users/lutherhart/Library/Mobile Documents/com~apple~CloudDocs/Quant Projects/lmhv_01"
VENV_PATH="$PROJECT_DIR/.venv/bin/activate"
SCRIPT_PATH="$PROJECT_DIR/collect_intraday_greeks.py"
LOG_DIR="$PROJECT_DIR/logs"

# Create logs directory
mkdir -p "$LOG_DIR"

# Cron entry (runs every 30 minutes during market hours, Mon-Fri)
CRON_ENTRY="*/30 9-16 * * 1-5 cd \"$PROJECT_DIR\" && source \"$VENV_PATH\" && python \"$SCRIPT_PATH\" >> \"$LOG_DIR/greeks_collection.log\" 2>&1"

echo "=================================="
echo "Intraday Greeks Collection Setup"
echo "=================================="
echo ""
echo "This will add a cron job to collect Greeks every 30 minutes during:"
echo "  • 9:30am - 4:00pm ET"
echo "  • Monday - Friday"
echo ""
echo "Data will be saved to: data/intraday_greeks/YYYY-MM-DD/"
echo "Logs will be saved to: logs/greeks_collection.log"
echo ""
echo "Cron entry:"
echo "$CRON_ENTRY"
echo ""
echo "To install:"
echo "  1. Copy the line above"
echo "  2. Run: crontab -e"
echo "  3. Paste the line"
echo "  4. Save and exit"
echo ""
echo "Or run automatically (careful - edits your crontab):"
echo "  (crontab -l 2>/dev/null; echo \"$CRON_ENTRY\") | crontab -"
echo ""
echo "To test manually right now:"
echo "  cd \"$PROJECT_DIR\""
echo "  source \"$VENV_PATH\""
echo "  python \"$SCRIPT_PATH\""
echo ""

