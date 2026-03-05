"""
Dashboard Configuration
=======================
Central config for database paths, refresh intervals, and display settings.
Both databases are opened READ-ONLY — the dashboard never writes.

Setup: copy .env.example to .env and fill in your paths.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# DATABASE PATHS (from .env)
# ============================================================

TWAP_DB_PATH = os.getenv("TWAP_DB_PATH", "data/twap.db")
POLYMARKET_DB_PATH = os.getenv("POLYMARKET_DB_PATH", "data/polymarket.db")

# ============================================================
# DISPLAY SETTINGS
# ============================================================

# Coins to track on the trading page
TRACKED_COINS = ["HYPE", "BTC"]

# Default lookback for charts
DEFAULT_CHART_HOURS = 24

# ============================================================
# REFRESH
# ============================================================

# Auto-refresh interval in seconds (Streamlit rerun)
REFRESH_INTERVAL_SECONDS = 60

# ============================================================
# TRAILBOT CONFIG (mirror from signal_monitor.py for display)
# ============================================================

TRAILBOT = {
    "coin": "HYPE",
    "entry_zscore": 1.0,
    "entry_zscore_max": 2.0,
    "exit_zscore": 0.0,
    "fixed_stop_pct": 2.5,
    "trailing_stop_pct": 2.0,
    "lookback_bins": 24,
    "bin_size": "30min",
    "cap": 5000,
    "state_file": os.getenv("TRAILBOT_STATE_FILE", "data/signal_state.json"),
    "log_file": os.getenv("TRAILBOT_LOG_FILE", "data/signal_monitor.log"),
}