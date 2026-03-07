"""
TWAP Database Reader
====================
Read-only queries against twap.db for the trading section.
Covers: prices, market data, TWAP orders, signals, and TrailBot status.
"""

import sqlite3
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List

from config import TWAP_DB_PATH, TRAILBOT


def _connect() -> sqlite3.Connection:
    """Read-only connection to twap.db."""
    return sqlite3.connect(f"file:{TWAP_DB_PATH}?mode=ro", uri=True)


# ============================================================
# COIN DISCOVERY
# ============================================================

def get_all_coins() -> List[str]:
    """Get all coins available in market_snapshots, sorted alphabetically."""
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT DISTINCT coin FROM market_snapshots ORDER BY coin"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


# ============================================================
# PRICES
# ============================================================

def get_latest_price(coin: str) -> Optional[dict]:
    """Latest price + timestamp for a coin from snapshots."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT timestamp, price FROM snapshots "
            "WHERE symbol = ? AND price IS NOT NULL AND price != '' "
            "ORDER BY timestamp DESC LIMIT 1",
            (coin,)
        ).fetchone()
        if row:
            return {"timestamp": row[0], "price": row[1]}
        return None
    finally:
        conn.close()


def get_price_history(coin: str, hours: int = 24) -> pd.DataFrame:
    """Price history for charting from market_snapshots (1-min resolution)."""
    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        df = pd.read_sql_query(
            "SELECT snapshot_time as timestamp, mark_px as price "
            "FROM market_snapshots "
            "WHERE coin = ? AND snapshot_time >= ? AND mark_px IS NOT NULL "
            "ORDER BY snapshot_time",
            conn, params=[coin, cutoff]
        )
        if len(df) > 0:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', utc=True)
        return df
    finally:
        conn.close()


# ============================================================
# MARKET DATA
# ============================================================

def get_latest_market(coin: str) -> Optional[dict]:
    """Latest market snapshot: OI, funding, premium, volume."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT snapshot_time, mark_px, funding_8h, open_interest, "
            "day_ntl_vlm, premium, prev_day_px, open_interest_usd FROM market_snapshots "
            "WHERE coin = ? ORDER BY snapshot_time DESC LIMIT 1",
            (coin,)
        ).fetchone()
        if row:
            return {
                "snapshot_time": row[0],
                "mark_px": row[1],
                "funding_8h": row[2],
                "open_interest": row[3],
                "day_volume": row[4],
                "premium": row[5],
                "prev_day_px": row[6],
                "open_interest_usd": row[7],
            }
        return None
    finally:
        conn.close()


def get_market_history(coin: str, hours: int = 24) -> pd.DataFrame:
    """Market data history for OI / funding / volume charts."""
    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        df = pd.read_sql_query(
            "SELECT snapshot_time, mark_px, funding_8h, open_interest, "
            "open_interest_usd, day_ntl_vlm, premium FROM market_snapshots "
            "WHERE coin = ? AND snapshot_time >= ? ORDER BY snapshot_time",
            conn, params=[coin, cutoff]
        )
        if len(df) > 0:
            df['snapshot_time'] = pd.to_datetime(df['snapshot_time'], format='mixed', utc=True)
        return df
    finally:
        conn.close()


# ============================================================
# TOP GAINERS / LOSERS
# ============================================================

def get_top_movers_market(limit: int = 10) -> pd.DataFrame:
    """
    Top gainers and losers based on mark_px vs prev_day_px.
    Returns all coins sorted by change_pct, both directions.
    """
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT coin,
                   mark_px as price,
                   prev_day_px,
                   ROUND((mark_px - prev_day_px) / prev_day_px * 100, 2) as change_pct,
                   open_interest_usd as oi_usd,
                   day_ntl_vlm as volume,
                   funding_8h
            FROM market_snapshots
            WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM market_snapshots)
            AND prev_day_px > 0
            AND mark_px > 0
            ORDER BY change_pct DESC
            """, conn
        )
        return df
    finally:
        conn.close()


def get_top_gainers(limit: int = 10) -> pd.DataFrame:
    """Top N gainers by 24h change."""
    df = get_top_movers_market()
    return df.head(limit)


def get_top_losers(limit: int = 10) -> pd.DataFrame:
    """Top N losers by 24h change."""
    df = get_top_movers_market()
    return df.tail(limit).sort_values('change_pct', ascending=True)


# ============================================================
# TWAP ORDERS (signal source)
# ============================================================

def get_recent_orders(coin: str, hours: int = 24) -> pd.DataFrame:
    """Recent TWAP orders for a coin."""
    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        df = pd.read_sql_query(
            "SELECT address, side, size, product_type, "
            "duration_minutes, status, first_seen_at, last_seen_at "
            "FROM orders "
            "WHERE symbol = ? AND first_seen_at >= ? ORDER BY first_seen_at DESC",
            conn, params=[coin, cutoff]
        )
        if len(df) > 0:
            df['first_seen_at'] = pd.to_datetime(df['first_seen_at'], format='mixed', utc=True)
        return df
    finally:
        conn.close()


def get_order_flow_bins(coin: str, hours: int = 24, bin_size: str = "30min") -> pd.DataFrame:
    """
    Aggregate TWAP orders into capped flow bins.
    Mirrors the signal_monitor.py logic exactly.
    """
    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        cap = TRAILBOT["cap"]

        orders = pd.read_sql_query(
            "SELECT address, side, size, first_seen_at FROM orders "
            "WHERE symbol = ? AND status = 'completed' AND first_seen_at >= ?",
            conn, params=[coin, cutoff]
        )

        if len(orders) == 0:
            return pd.DataFrame(columns=['bin', 'capped_flow', 'unique_addrs', 'order_count'])

        orders['first_seen_at'] = pd.to_datetime(orders['first_seen_at'], format='mixed', utc=True)
        orders['bin'] = orders['first_seen_at'].dt.floor(bin_size)
        orders['signed_size'] = orders['size'] * orders['side'].map({'BUY': 1, 'SELL': -1})

        # Capped flow per address per bin
        addr_bin = orders.groupby(['bin', 'address'])['signed_size'].sum().reset_index()
        addr_bin['capped'] = addr_bin['signed_size'].clip(-cap, cap)

        # Aggregate per bin
        flow = addr_bin.groupby('bin').agg(
            capped_flow=('capped', 'sum'),
            unique_addrs=('address', 'nunique'),
        ).reset_index()

        order_counts = orders.groupby('bin').size().reset_index(name='order_count')
        flow = flow.merge(order_counts, on='bin', how='left')

        return flow.sort_values('bin').reset_index(drop=True)
    finally:
        conn.close()


# ============================================================
# TWAP PRESSURE (from snapshots table)
# ============================================================

def get_pressure_history(coin: str, hours: int = 24) -> pd.DataFrame:
    """Net pressure over time from TWAP snapshots."""
    conn = _connect()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        df = pd.read_sql_query(
            """
            SELECT timestamp, price, net_pressure,
                   perp_buy_pressure, perp_sell_pressure,
                   spot_buy_pressure, spot_sell_pressure,
                   unique_addresses, active_orders
            FROM snapshots
            WHERE symbol = ? AND timestamp >= ?
            ORDER BY timestamp
            """,
            conn, params=[coin, cutoff]
        )
        if len(df) > 0:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', utc=True)
        return df
    finally:
        conn.close()


# ============================================================
# TRAILBOT STATUS
# ============================================================

def get_trailbot_state() -> Optional[dict]:
    """Read the current TrailBot state from signal_state.json."""
    state_path = Path(TRAILBOT["state_file"])
    if state_path.exists():
        try:
            with open(state_path, 'r') as f:
                return json.load(f)
        except Exception:
            return None
    return None


def get_trailbot_recent_log(n_lines: int = 50) -> list:
    """Read the last N lines from the TrailBot log."""
    log_path = Path(TRAILBOT["log_file"])
    if not log_path.exists():
        return []
    try:
        with open(log_path, 'r') as f:
            lines = f.readlines()
            return lines[-n_lines:]
    except Exception:
        return []


def get_trailbot_trades_today() -> list:
    """Parse today's ENTRY/EXIT events from the log."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    log_lines = get_trailbot_recent_log(500)
    trades = []
    for line in log_lines:
        if today in line and any(kw in line for kw in ['ENTRY:', 'EXIT:', 'Position:']):
            trades.append(line.strip())
    return trades