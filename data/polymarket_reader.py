"""
Polymarket Database Reader
==========================
Read-only queries against polymarket.db for the monitoring section.
Covers: market snapshots, price movements, alerts, top movers,
theme-level aggregation, and money flow analysis.
"""

import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Optional

from config import POLYMARKET_DB_PATH

# ── Theme → tag_slug mapping (mirrors themes/*.json) ──
THEME_TAG_SLUGS = {
    "Fed & Monetary Policy": [
        "fed", "fomc", "interest-rates", "inflation",
        "cpi", "monetary-policy", "federal-reserve",
    ],
    "Geopolitics": ["iran", "nuclear", "oil", "geopolitics"],
    "Trade & Tariffs": ["tariffs", "trade-war"],
}


def _connect() -> sqlite3.Connection:
    """Read-only connection to polymarket.db."""
    return sqlite3.connect(f"file:{POLYMARKET_DB_PATH}?mode=ro", uri=True)


# ============================================================
# MARKET OVERVIEW
# ============================================================

def get_active_markets() -> pd.DataFrame:
    """
    Get the latest snapshot for each tracked market.
    Returns one row per condition_id with the most recent data.
    """
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT s.event_slug, s.question, s.condition_id,
                   s.yes_prob, s.best_bid, s.best_ask, s.spread,
                   s.volume_24h, s.total_volume, s.liquidity,
                   s.change_1d, s.change_1w, s.snapshot_time
            FROM snapshots s
            INNER JOIN (
                SELECT condition_id, MAX(snapshot_time) as max_time
                FROM snapshots
                GROUP BY condition_id
            ) latest ON s.condition_id = latest.condition_id
                    AND s.snapshot_time = latest.max_time
            ORDER BY s.volume_24h DESC
            """, conn
        )
        return df
    finally:
        conn.close()


def get_top_movers(hours: int = 24, min_move_pct: float = 3.0) -> pd.DataFrame:
    """
    Find markets with the biggest probability moves in the last N hours.
    Compares current yes_prob to the earliest snapshot in the window.
    """
    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

        df = pd.read_sql_query(
            """
            WITH current AS (
                SELECT condition_id, question, event_slug, yes_prob,
                       volume_24h, snapshot_time,
                       ROW_NUMBER() OVER (PARTITION BY condition_id ORDER BY snapshot_time DESC) as rn
                FROM snapshots
            ),
            baseline AS (
                SELECT condition_id, yes_prob as old_prob, snapshot_time as old_time,
                       ROW_NUMBER() OVER (PARTITION BY condition_id ORDER BY snapshot_time ASC) as rn
                FROM snapshots
                WHERE snapshot_time >= ?
            )
            SELECT c.condition_id, c.question, c.event_slug,
                   b.old_prob, c.yes_prob as current_prob,
                   (c.yes_prob - b.old_prob) as move_pct,
                   c.volume_24h,
                   b.old_time, c.snapshot_time as current_time
            FROM current c
            JOIN baseline b ON c.condition_id = b.condition_id AND b.rn = 1
            WHERE c.rn = 1
            AND ABS(c.yes_prob - b.old_prob) >= ?
            ORDER BY ABS(c.yes_prob - b.old_prob) DESC
            """, conn, params=[cutoff, min_move_pct]
        )
        return df
    finally:
        conn.close()


# ============================================================
# MONEY FLOW — "Where is the money going?"
# ============================================================

def get_money_flow(top_n: int = 25) -> pd.DataFrame:
    """
    Top markets by 24h volume with probability direction.
    This is the "where is the money going" view.
    """
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT s.event_slug, s.question, s.condition_id,
                   s.yes_prob, s.volume_24h, s.total_volume,
                   s.liquidity, s.spread, s.change_1d,
                   s.snapshot_time
            FROM snapshots s
            INNER JOIN (
                SELECT condition_id, MAX(snapshot_time) as max_time
                FROM snapshots
                GROUP BY condition_id
            ) latest ON s.condition_id = latest.condition_id
                    AND s.snapshot_time = latest.max_time
            WHERE s.volume_24h > 0
            ORDER BY s.volume_24h DESC
            LIMIT ?
            """, conn, params=[top_n]
        )
        return df
    finally:
        conn.close()


def get_theme_summary() -> pd.DataFrame:
    """
    Aggregate stats per theme: total volume, market count,
    number of movers (>3% in 24h).
    Uses event_slug matching against known theme tag_slugs.
    """
    conn = _connect()
    try:
        # Get latest snapshot per market
        markets = pd.read_sql_query(
            """
            SELECT s.event_slug, s.condition_id, s.yes_prob,
                   s.volume_24h, s.total_volume, s.liquidity,
                   s.change_1d, s.snapshot_time
            FROM snapshots s
            INNER JOIN (
                SELECT condition_id, MAX(snapshot_time) as max_time
                FROM snapshots
                GROUP BY condition_id
            ) latest ON s.condition_id = latest.condition_id
                    AND s.snapshot_time = latest.max_time
            """, conn
        )
        return markets
    finally:
        conn.close()


def get_volume_by_event(top_n: int = 15) -> pd.DataFrame:
    """
    Aggregate 24h volume by event (not individual market).
    Shows which events (stories) are attracting the most money.
    """
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT s.event_slug,
                   COUNT(DISTINCT s.condition_id) as market_count,
                   ROUND(SUM(s.volume_24h), 0) as total_24h_vol,
                   ROUND(SUM(s.total_volume), 0) as lifetime_vol,
                   ROUND(AVG(s.yes_prob), 1) as avg_prob,
                   ROUND(SUM(s.liquidity), 0) as total_liquidity
            FROM snapshots s
            INNER JOIN (
                SELECT condition_id, MAX(snapshot_time) as max_time
                FROM snapshots
                GROUP BY condition_id
            ) latest ON s.condition_id = latest.condition_id
                    AND s.snapshot_time = latest.max_time
            GROUP BY s.event_slug
            HAVING total_24h_vol > 0
            ORDER BY total_24h_vol DESC
            LIMIT ?
            """, conn, params=[top_n]
        )
        return df
    finally:
        conn.close()


def get_prob_timeline(condition_ids: list, hours: int = 24) -> pd.DataFrame:
    """
    Probability timeline for multiple markets (for overlay charts).
    """
    if not condition_ids:
        return pd.DataFrame()

    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        placeholders = ','.join(['?'] * len(condition_ids))
        df = pd.read_sql_query(
            f"""
            SELECT snapshot_time, condition_id, question, yes_prob
            FROM snapshots
            WHERE condition_id IN ({placeholders})
            AND snapshot_time >= ?
            ORDER BY snapshot_time
            """, conn, params=[*condition_ids, cutoff]
        )
        if len(df) > 0:
            df['snapshot_time'] = pd.to_datetime(df['snapshot_time'], format='mixed', utc=True)
        return df
    finally:
        conn.close()


# ============================================================
# MARKET DETAIL
# ============================================================

def get_market_history(condition_id: str, hours: int = 48) -> pd.DataFrame:
    """Price history for a specific market (for charting)."""
    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        df = pd.read_sql_query(
            "SELECT snapshot_time, yes_prob, best_bid, best_ask, spread, "
            "volume_24h FROM snapshots "
            "WHERE condition_id = ? AND snapshot_time >= ? ORDER BY snapshot_time",
            conn, params=[condition_id, cutoff]
        )
        if len(df) > 0:
            df['snapshot_time'] = pd.to_datetime(df['snapshot_time'], format='mixed', utc=True)
        return df
    finally:
        conn.close()


def get_markets_by_event(event_slug: str) -> pd.DataFrame:
    """Get all markets (conditions) under an event slug, latest snapshot each."""
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT s.question, s.condition_id, s.yes_prob, s.best_bid,
                   s.volume_24h, s.total_volume, s.change_1d, s.snapshot_time
            FROM snapshots s
            INNER JOIN (
                SELECT condition_id, MAX(snapshot_time) as max_time
                FROM snapshots
                WHERE event_slug = ?
                GROUP BY condition_id
            ) latest ON s.condition_id = latest.condition_id
                    AND s.snapshot_time = latest.max_time
            ORDER BY s.yes_prob DESC
            """, conn, params=[event_slug,]
        )
        return df
    finally:
        conn.close()


# ============================================================
# ALERTS
# ============================================================

def get_recent_alerts(hours: int = 24) -> pd.DataFrame:
    """Recent alerts from the Polymarket monitor."""
    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        df = pd.read_sql_query(
            "SELECT alert_time, alert_type, event_slug, question, "
            "prob_now, prob_before, volume_now, details FROM alerts "
            "WHERE alert_time >= ? ORDER BY alert_time DESC",
            conn, params=[cutoff,]
        )
        return df
    finally:
        conn.close()


# ============================================================
# EVENT SLUGS / DISCOVERY
# ============================================================

def get_tracked_event_slugs() -> list:
    """Get all unique event slugs currently being tracked."""
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT DISTINCT event_slug FROM snapshots ORDER BY event_slug"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def get_event_summary() -> pd.DataFrame:
    """Summary stats per event: market count, avg prob, total volume."""
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT s.event_slug,
                   COUNT(DISTINCT s.condition_id) as market_count,
                   ROUND(AVG(s.yes_prob), 3) as avg_prob,
                   ROUND(SUM(s.volume_24h), 0) as total_24h_vol,
                   MAX(s.snapshot_time) as last_update
            FROM snapshots s
            INNER JOIN (
                SELECT condition_id, MAX(snapshot_time) as max_time
                FROM snapshots GROUP BY condition_id
            ) latest ON s.condition_id = latest.condition_id
                    AND s.snapshot_time = latest.max_time
            GROUP BY s.event_slug
            ORDER BY total_24h_vol DESC
            """, conn
        )
        return df
    finally:
        conn.close()