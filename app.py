"""
Dashboard v1.0
==============
Single Streamlit dashboard reading from both twap.db and polymarket.db.

Run: streamlit run app.py --server.port 8501
"""

import streamlit as st

from datetime import datetime, timezone

from config import TRACKED_COINS, REFRESH_INTERVAL_SECONDS, TRAILBOT
from data.twap_reader import (
    get_latest_price, get_price_history, get_latest_market,
    get_market_history, get_order_flow_bins, get_trailbot_state,
    get_trailbot_trades_today,
)
from data.polymarket_reader import (
    get_active_markets, get_top_movers, get_recent_alerts,
    get_event_summary, get_market_history as get_pm_market_history,
    get_markets_by_event,
)


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Trading Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# SIDEBAR NAVIGATION
# ============================================================

with st.sidebar:
    st.title("📊 Dashboard")
    st.caption("twap.db + polymarket.db")

    page = st.radio(
        "Navigate",
        ["Overview", "Trading", "Polymarket"],
        index=0,
    )

    st.divider()

    # Refresh controls
    auto_refresh = st.toggle("Auto-refresh", value=False)
    if auto_refresh:
        st.caption(f"Refreshing every {REFRESH_INTERVAL_SECONDS}s")

    if st.button("🔄 Refresh now"):
        st.rerun()

    st.divider()
    st.caption(f"Last loaded: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")


# ============================================================
# PAGE: OVERVIEW
# ============================================================

def render_overview():
    st.header("Overview")

    # ---- Row 1: Prices + TrailBot status ----
    col1, col2, col3 = st.columns(3)

    # HYPE price
    with col1:
        hype = get_latest_price("HYPE")
        hype_market = get_latest_market("HYPE")
        if hype:
            st.metric("HYPE", f"${hype['price']:.4f}")
        if hype_market:
            oi_m = hype_market['open_interest'] / 1e6
            vol_m = hype_market['day_volume'] / 1e6
            st.caption(f"OI: {oi_m:.1f}M | Vol: ${vol_m:.0f}M | Fund: {hype_market['funding_8h']*100:+.4f}%")

    # BTC price
    with col2:
        btc = get_latest_price("BTC")
        btc_market = get_latest_market("BTC")
        if btc:
            st.metric("BTC", f"${btc['price']:,.0f}")
        if btc_market:
            oi_b = btc_market['open_interest'] * btc_market['mark_px'] / 1e9
            st.caption(f"OI: ${oi_b:.2f}B | Fund: {btc_market['funding_8h']*100:+.4f}%")

    # TrailBot status
    with col3:
        state = get_trailbot_state()
        if state:
            trade = state.get('trade')
            last_signal = state.get('last_signal', {})
            if trade:
                st.metric("TrailBot", f"🟢 {trade['direction'].upper()}")
                st.caption(f"Entry: ${trade['entry_price']:.4f} | Max: ${trade.get('max_price', 0):.4f}")
            else:
                z = last_signal.get('cf_z', 0)
                st.metric("TrailBot", "⚪ No position")
                st.caption(f"Last signal: z={z:+.2f} ({last_signal.get('direction', 'neutral')})")
        else:
            st.metric("TrailBot", "❓ Unknown")

    st.divider()

    # ---- Row 2: Today's trades + Polymarket movers ----
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("TrailBot — Today's Trades")
        trades = get_trailbot_trades_today()
        if trades:
            for t in trades:
                st.text(t)
        else:
            st.info("No trades yet today.")

    with col_right:
        st.subheader("Polymarket — Top Movers (24h)")
        try:
            movers = get_top_movers(hours=24, min_move_pct=3.0)
            if len(movers) > 0:
                for _, row in movers.head(10).iterrows():
                    direction = "🟢" if row['move_pct'] > 0 else "🔴"
                    st.text(f"{direction} {row['move_pct']:+.1f}% | {row['question'][:60]}")
            else:
                st.info("No significant moves in the last 24h.")
        except Exception as e:
            st.warning(f"Polymarket data unavailable: {e}")


# ============================================================
# PAGE: TRADING
# ============================================================

def render_trading():
    st.header("Trading")

    # Controls
    col1, col2 = st.columns([1, 3])
    with col1:
        coin = st.selectbox("Coin", TRACKED_COINS, index=0)
        hours = st.selectbox("Lookback", [6, 12, 24, 48, 72], index=2)

    # ---- Price chart ----
    st.subheader(f"{coin} Price — Last {hours}h")
    price_df = get_price_history(coin, hours)
    if len(price_df) > 0:
        st.line_chart(price_df.set_index('timestamp')['price'])
    else:
        st.warning("No price data available.")

    st.divider()

    # ---- Market data ----
    col_oi, col_fund = st.columns(2)

    market_df = get_market_history(coin, hours)
    if len(market_df) > 0:
        with col_oi:
            st.subheader("Open Interest")
            st.line_chart(market_df.set_index('snapshot_time')['open_interest'])

        with col_fund:
            st.subheader("Funding Rate (8h)")
            st.line_chart(market_df.set_index('snapshot_time')['funding_8h'])

    st.divider()

    # ---- TWAP Order Flow ----
    st.subheader(f"Capped Flow Bins — {coin}")
    flow_df = get_order_flow_bins(coin, hours)
    if len(flow_df) > 0:
        st.bar_chart(flow_df.set_index('bin')['capped_flow'])

        # Show address count alongside
        st.caption("Unique addresses per bin:")
        st.bar_chart(flow_df.set_index('bin')['unique_addrs'])
    else:
        st.info("No TWAP orders in this window.")

    st.divider()

    # ---- TrailBot config reference ----
    with st.expander("TrailBot v3.0 Config"):
        for key, val in TRAILBOT.items():
            if key not in ('state_file', 'log_file'):
                st.text(f"{key}: {val}")


# ============================================================
# PAGE: POLYMARKET
# ============================================================

def render_polymarket():
    st.header("Polymarket Monitor")

    # ---- Event summary ----
    st.subheader("Tracked Events")
    try:
        events = get_event_summary()
        if len(events) > 0:
            st.dataframe(
                events.rename(columns={
                    'event_slug': 'Event',
                    'market_count': 'Markets',
                    'avg_prob': 'Avg Prob',
                    'total_24h_vol': '24h Volume',
                    'last_update': 'Last Update',
                }),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No events being tracked.")
    except Exception as e:
        st.warning(f"Polymarket data unavailable: {e}")
        return

    st.divider()

    # ---- Top movers ----
    col1, col2 = st.columns([1, 3])
    with col1:
        mover_hours = st.selectbox("Movers window", [6, 12, 24, 48], index=2)
        min_move = st.slider("Min move %", 1.0, 10.0, 3.0, 0.5)

    st.subheader(f"Top Movers — Last {mover_hours}h (>{min_move}%)")
    movers = get_top_movers(hours=mover_hours, min_move_pct=min_move)
    if len(movers) > 0:
        st.dataframe(
            movers[['question', 'event_slug', 'old_prob', 'current_prob', 'move_pct']].rename(columns={
                'question': 'Market',
                'event_slug': 'Event',
                'old_prob': 'Was',
                'current_prob': 'Now',
                'move_pct': 'Move %',
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No significant movers.")

    st.divider()

    # ---- Recent alerts ----
    st.subheader("Recent Alerts")
    alerts = get_recent_alerts(hours=24)
    if len(alerts) > 0:
        st.dataframe(
            alerts[['alert_time', 'alert_type', 'question', 'prob_now', 'prob_before']].rename(columns={
                'alert_time': 'Time',
                'alert_type': 'Type',
                'question': 'Market',
                'prob_now': 'Now',
                'prob_before': 'Before',
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No alerts in the last 24h.")

    st.divider()

    # ---- Drill into specific event ----
    st.subheader("Event Detail")
    try:
        slugs = get_event_summary()['event_slug'].tolist() if len(events) > 0 else []
        if slugs:
            selected_event = st.selectbox("Select event", slugs)
            if selected_event:
                markets = get_markets_by_event(selected_event)
                if len(markets) > 0:
                    st.dataframe(
                        markets[['question', 'yes_prob', 'best_bid', 'volume_24h', 'change_1d']].rename(columns={
                            'question': 'Market',
                            'yes_prob': 'Prob',
                            'best_bid': 'Bid',
                            'volume_24h': '24h Vol',
                            'change_1d': '1d Change',
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )
    except Exception as e:
        st.warning(f"Could not load event detail: {e}")


# ============================================================
# ROUTING
# ============================================================

if page == "Overview":
    render_overview()
elif page == "Trading":
    render_trading()
elif page == "Polymarket":
    render_polymarket()


# ============================================================
# AUTO-REFRESH
# ============================================================

if auto_refresh:
    import time
    time.sleep(REFRESH_INTERVAL_SECONDS)
    st.rerun()