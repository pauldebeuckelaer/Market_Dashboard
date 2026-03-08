"""
Dashboard v2.0
==============
Trading dashboard reading from twap.db and polymarket.db.
Altair charts, top movers, dynamic coin selection.

Run: streamlit run app.py --server.port 8501
"""

import json
import altair as alt
import streamlit as st
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from config import (
    PRIORITY_COINS, REFRESH_INTERVAL_SECONDS, TRAILBOT,
    DEFAULT_CHART_HOURS, SCANNER_STATUS_FILE,
)
from data.twap_reader import (
    get_all_coins, get_latest_price, get_price_history,
    get_latest_market, get_market_history,
    get_order_flow_bins, get_pressure_history,
    get_trailbot_state, get_trailbot_trades_today,
    get_top_gainers, get_top_losers, get_top_movers_market,
    get_recent_orders,
)
from data.polymarket_reader import (
    get_money_flow, get_theme_summary, get_volume_by_event,
    get_prob_timeline, THEME_TAG_SLUGS,
    get_active_markets, get_top_movers, get_recent_alerts,
    get_event_summary, get_market_history as get_pm_market_history,
    get_markets_by_event,
)

from data.polymarket_reader import (
    get_active_markets, get_top_movers, get_recent_alerts,
    get_event_summary, get_market_history as get_pm_market_history,
    get_markets_by_event, get_money_flow, get_theme_summary,
    get_volume_by_event, get_prob_timeline, get_consensus,
    THEME_TAG_SLUGS,
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
# SIDEBAR
# ============================================================

with st.sidebar:
    st.title("📊 Dashboard")
    st.caption("v2.0 — twap.db + polymarket.db")

    page = st.radio(
        "Navigate",
        ["Overview", "Trading", "Polymarket"],
        index=0,
    )

    st.divider()

    auto_refresh = st.toggle("Auto-refresh", value=False)
    if auto_refresh:
        st.caption(f"Refreshing every {REFRESH_INTERVAL_SECONDS}s")

    if st.button("🔄 Refresh now"):
        st.rerun()

    st.divider()
    st.caption(f"Last loaded: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")


# ============================================================
# HELPERS
# ============================================================

def format_price(price: float) -> str:
    """Smart price formatting based on magnitude."""
    if price is None:
        return "N/A"
    if price >= 1000:
        return f"${price:,.0f}"
    elif price >= 1:
        return f"${price:.2f}"
    elif price >= 0.01:
        return f"${price:.4f}"
    else:
        return f"${price:.6f}"


def format_volume(vol: float) -> str:
    """Format volume with K/M/B suffixes."""
    if vol is None:
        return "N/A"
    if vol >= 1e9:
        return f"${vol/1e9:.1f}B"
    elif vol >= 1e6:
        return f"${vol/1e6:.0f}M"
    elif vol >= 1e3:
        return f"${vol/1e3:.0f}K"
    return f"${vol:.0f}"


def get_coin_list() -> list:
    """Get coin list with priority coins first."""
    try:
        all_coins = get_all_coins()
        # Priority coins first, then the rest alphabetically
        priority = [c for c in PRIORITY_COINS if c in all_coins]
        rest = [c for c in all_coins if c not in PRIORITY_COINS]
        return priority + rest
    except Exception:
        return PRIORITY_COINS


def load_scanner_status() -> dict:
    """Load the scanner status JSON."""
    path = Path(SCANNER_STATUS_FILE)
    if path.exists():
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def make_price_chart(df: pd.DataFrame, height: int = 350) -> alt.Chart:
    """Create an Altair price line chart."""
    hover = alt.selection_point(
        fields=['timestamp'],
        nearest=True,
        on='pointerover',
        empty=False,
    )

    base = alt.Chart(df).encode(
        x=alt.X('timestamp:T', title=None, axis=alt.Axis(format='%H:%M', labelAngle=-45)),
        y=alt.Y('price:Q', title='Price', scale=alt.Scale(zero=False)),
    )

    line = base.mark_line(color='#4FC3F7', strokeWidth=2)

    points = base.mark_circle(size=60, color='#4FC3F7').encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)),
    ).add_params(hover)

    tooltips = base.mark_rule(color='gray', strokeDash=[4, 4]).encode(
        opacity=alt.condition(hover, alt.value(0.5), alt.value(0)),
        tooltip=[
            alt.Tooltip('timestamp:T', title='Time', format='%Y-%m-%d %H:%M'),
            alt.Tooltip('price:Q', title='Price', format=',.4f'),
        ]
    ).add_params(hover)

    return (line + points + tooltips).properties(height=height).interactive()


def make_volume_chart(df: pd.DataFrame, height: int = 120) -> alt.Chart:
    """Create an Altair volume bar chart."""
    return alt.Chart(df).mark_bar(color='#37474F', opacity=0.6).encode(
        x=alt.X('snapshot_time:T', title=None, axis=alt.Axis(format='%H:%M', labelAngle=-45)),
        y=alt.Y('day_ntl_vlm:Q', title='Volume'),
        tooltip=[
            alt.Tooltip('snapshot_time:T', title='Time', format='%H:%M'),
            alt.Tooltip('day_ntl_vlm:Q', title='Volume', format=',.0f'),
        ]
    ).properties(height=height).interactive()


def make_oi_chart(df: pd.DataFrame, height: int = 200) -> alt.Chart:
    """OI over time."""
    col = 'open_interest_usd' if 'open_interest_usd' in df.columns and df['open_interest_usd'].notna().any() else 'open_interest'
    return alt.Chart(df).mark_area(
        color='#26A69A', opacity=0.3, line={'color': '#26A69A', 'strokeWidth': 2}
    ).encode(
        x=alt.X('snapshot_time:T', title=None, axis=alt.Axis(format='%H:%M', labelAngle=-45)),
        y=alt.Y(f'{col}:Q', title='Open Interest', scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip('snapshot_time:T', title='Time', format='%H:%M'),
            alt.Tooltip(f'{col}:Q', title='OI', format=',.0f'),
        ]
    ).properties(height=height).interactive()


def make_funding_chart(df: pd.DataFrame, height: int = 200) -> alt.Chart:
    """Funding rate over time with color coding."""
    df = df.copy()
    df['funding_pct'] = df['funding_8h'] * 100

    return alt.Chart(df).mark_bar().encode(
        x=alt.X('snapshot_time:T', title=None, axis=alt.Axis(format='%H:%M', labelAngle=-45)),
        y=alt.Y('funding_pct:Q', title='Funding 8h (%)'),
        color=alt.condition(
            alt.datum.funding_pct > 0,
            alt.value('#26A69A'),
            alt.value('#EF5350'),
        ),
        tooltip=[
            alt.Tooltip('snapshot_time:T', title='Time', format='%H:%M'),
            alt.Tooltip('funding_pct:Q', title='Funding %', format='.5f'),
        ]
    ).properties(height=height).interactive()


def make_pressure_chart(df: pd.DataFrame, height: int = 200) -> alt.Chart:
    """Net TWAP pressure over time."""
    return alt.Chart(df).mark_bar().encode(
        x=alt.X('timestamp:T', title=None, axis=alt.Axis(format='%H:%M', labelAngle=-45)),
        y=alt.Y('net_pressure:Q', title='Net Pressure'),
        color=alt.condition(
            alt.datum.net_pressure > 0,
            alt.value('#26A69A'),
            alt.value('#EF5350'),
        ),
        tooltip=[
            alt.Tooltip('timestamp:T', title='Time', format='%H:%M'),
            alt.Tooltip('net_pressure:Q', title='Net Pressure', format=',.2f'),
        ]
    ).properties(height=height).interactive()


# ============================================================
# PAGE: OVERVIEW
# ============================================================

def render_overview():
    st.header("Overview")

    # ---- Row 1: Key prices ----
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        hype_mkt = get_latest_market("HYPE")
        if hype_mkt:
            change = 0
            if hype_mkt.get('prev_day_px') and hype_mkt['prev_day_px'] > 0:
                change = ((hype_mkt['mark_px'] - hype_mkt['prev_day_px']) / hype_mkt['prev_day_px']) * 100
            st.metric("HYPE", format_price(hype_mkt['mark_px']), f"{change:+.2f}%")
            oi_m = (hype_mkt.get('open_interest_usd') or 0) / 1e6
            vol_m = (hype_mkt.get('day_volume') or 0) / 1e6
            st.caption(f"OI: ${oi_m:.0f}M | Vol: ${vol_m:.0f}M")

    with col2:
        btc_mkt = get_latest_market("BTC")
        if btc_mkt:
            change = 0
            if btc_mkt.get('prev_day_px') and btc_mkt['prev_day_px'] > 0:
                change = ((btc_mkt['mark_px'] - btc_mkt['prev_day_px']) / btc_mkt['prev_day_px']) * 100
            st.metric("BTC", format_price(btc_mkt['mark_px']), f"{change:+.2f}%")
            oi_b = (btc_mkt.get('open_interest_usd') or 0) / 1e9
            st.caption(f"OI: ${oi_b:.2f}B")

    with col3:
        eth_mkt = get_latest_market("ETH")
        if eth_mkt:
            change = 0
            if eth_mkt.get('prev_day_px') and eth_mkt['prev_day_px'] > 0:
                change = ((eth_mkt['mark_px'] - eth_mkt['prev_day_px']) / eth_mkt['prev_day_px']) * 100
            st.metric("ETH", format_price(eth_mkt['mark_px']), f"{change:+.2f}%")

    with col4:
        sol_mkt = get_latest_market("SOL")
        if sol_mkt:
            change = 0
            if sol_mkt.get('prev_day_px') and sol_mkt['prev_day_px'] > 0:
                change = ((sol_mkt['mark_px'] - sol_mkt['prev_day_px']) / sol_mkt['prev_day_px']) * 100
            st.metric("SOL", format_price(sol_mkt['mark_px']), f"{change:+.2f}%")

    st.divider()

    # ---- Row 2: TrailBot + Scanner ----
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("TrailBot")
        state = get_trailbot_state()
        if state:
            trade = state.get('trade')
            last_signal = state.get('last_signal', {})
            if trade:
                st.success(f"🟢 {trade['direction'].upper()} — Entry: ${trade['entry_price']:.4f}")
            else:
                z = last_signal.get('cf_z', 0)
                st.info(f"⚪ No position — Last signal: z={z:+.2f}")

            trades = get_trailbot_trades_today()
            if trades:
                st.caption("Today's trades:")
                for t in trades:
                    st.text(t)
        else:
            st.warning("TrailBot status unavailable")

    with col_right:
        st.subheader("Activity Scanner")
        scanner = load_scanner_status()
        if scanner and scanner.get('opportunities'):
            for opp in scanner['opportunities'][:5]:
                symbol = opp['symbol']
                ratio = opp['activity_ratio']
                direction = opp['direction']
                buy_pct = opp['buy_pct']
                price_chg = opp['price_chg_pct']
                status = opp['status']

                icon = "🔥" if status == "HOT" else "📊"
                dir_icon = "🟢" if direction == "BUY" else "🔴" if direction == "SELL" else "⚪"

                st.text(f"{icon} {symbol:6s} {ratio:5.1f}x {dir_icon} {direction:7s} {buy_pct:3.0f}% buy  {price_chg:+6.2f}%")

            scan_time = scanner.get('scan_time', '')
            if scan_time:
                st.caption(f"Last scan: {scan_time[:19]}")
        else:
            st.info("No scanner data available")

    st.divider()

    # ---- Row 3: Top Gainers / Losers ----
    col_gain, col_lose = st.columns(2)

    with col_gain:
        st.subheader("Top Gainers (24h)")
        try:
            gainers = get_top_gainers(8)
            if len(gainers) > 0:
                for _, row in gainers.iterrows():
                    st.text(
                        f"🟢 {row['coin']:8s} {row['change_pct']:+6.2f}%  "
                        f"{format_price(row['price']):>12s}  "
                        f"Vol: {format_volume(row['volume'])}"
                    )
        except Exception as e:
            st.warning(f"Could not load gainers: {e}")

    with col_lose:
        st.subheader("Top Losers (24h)")
        try:
            losers = get_top_losers(8)
            if len(losers) > 0:
                for _, row in losers.iterrows():
                    st.text(
                        f"🔴 {row['coin']:8s} {row['change_pct']:+6.2f}%  "
                        f"{format_price(row['price']):>12s}  "
                        f"Vol: {format_volume(row['volume'])}"
                    )
        except Exception as e:
            st.warning(f"Could not load losers: {e}")

    st.divider()

    # ---- Row 4: Polymarket movers ----
    st.subheader("Polymarket — Top Movers (24h)")
    try:
        movers = get_top_movers(hours=24, min_move_pct=3.0)
        if len(movers) > 0:
            for _, row in movers.head(8).iterrows():
                direction = "🟢" if row['move_pct'] > 0 else "🔴"
                st.text(f"{direction} {row['move_pct']:+6.1f}% | {row['question'][:65]}")
        else:
            st.info("No significant moves in the last 24h.")
    except Exception as e:
        st.warning(f"Polymarket data unavailable: {e}")


# ============================================================
# PAGE: TRADING
# ============================================================

def render_trading():
    st.header("Trading")

    # ---- Controls ----
    col1, col2 = st.columns([1, 3])
    with col1:
        coins = get_coin_list()
        coin = st.selectbox("Coin", coins, index=0)
        hours = st.selectbox("Lookback (hours)", [6, 12, 24, 48, 72], index=2)

    # ---- Market summary for selected coin ----
    mkt = get_latest_market(coin)
    if mkt:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            change = 0
            if mkt.get('prev_day_px') and mkt['prev_day_px'] > 0:
                change = ((mkt['mark_px'] - mkt['prev_day_px']) / mkt['prev_day_px']) * 100
            st.metric("Price", format_price(mkt['mark_px']), f"{change:+.2f}%")
        with c2:
            oi_val = mkt.get('open_interest_usd') or (mkt.get('open_interest', 0) * (mkt.get('mark_px') or 0))
            st.metric("Open Interest", format_volume(oi_val))
        with c3:
            st.metric("24h Volume", format_volume(mkt.get('day_volume')))
        with c4:
            funding = (mkt.get('funding_8h') or 0) * 100
            st.metric("Funding 8h", f"{funding:+.4f}%")

    st.divider()

    # ---- Price chart ----
    st.subheader(f"{coin} Price")
    price_df = get_price_history(coin, hours)
    if len(price_df) > 0:
        st.altair_chart(make_price_chart(price_df, height=400), use_container_width=True)
    else:
        st.warning("No price data available.")

    # ---- Volume chart ----
    market_df = get_market_history(coin, hours)
    if len(market_df) > 0 and 'day_ntl_vlm' in market_df.columns:
        st.subheader("Volume")
        st.altair_chart(make_volume_chart(market_df, height=150), use_container_width=True)

    st.divider()

    # ---- OI + Funding side by side ----
    if len(market_df) > 0:
        col_oi, col_fund = st.columns(2)
        with col_oi:
            st.subheader("Open Interest")
            st.altair_chart(make_oi_chart(market_df, height=250), use_container_width=True)
        with col_fund:
            st.subheader("Funding Rate")
            st.altair_chart(make_funding_chart(market_df, height=250), use_container_width=True)

    st.divider()

    # ---- TWAP Pressure ----
    st.subheader(f"TWAP Net Pressure — {coin}")
    pressure_df = get_pressure_history(coin, hours)
    if len(pressure_df) > 0:
        st.altair_chart(make_pressure_chart(pressure_df, height=250), use_container_width=True)

        # Summary stats
        c1, c2, c3 = st.columns(3)
        with c1:
            total_buy = pressure_df['perp_buy_pressure'].sum() + pressure_df['spot_buy_pressure'].sum()
            st.metric("Total Buy Pressure", f"{total_buy:,.0f}")
        with c2:
            total_sell = pressure_df['perp_sell_pressure'].sum() + pressure_df['spot_sell_pressure'].sum()
            st.metric("Total Sell Pressure", f"{total_sell:,.0f}")
        with c3:
            net = total_buy - total_sell
            st.metric("Net", f"{net:+,.0f}")
    else:
        st.info("No TWAP data for this coin in the selected window.")

    st.divider()

    # ---- Recent TWAP Orders ----
    st.subheader(f"Recent TWAP Orders — {coin}")
    orders_df = get_recent_orders(coin, hours)
    if len(orders_df) > 0:
        # Shorten address for display
        orders_df['address'] = orders_df['address'].apply(lambda x: f"{x[:6]}...{x[-4:]}" if len(x) > 10 else x)
        st.dataframe(
            orders_df[['address', 'side', 'size', 'product_type', 'duration_minutes', 'status', 'first_seen_at']].rename(columns={
                'address': 'Address',
                'side': 'Side',
                'size': 'Size',
                'product_type': 'Type',
                'duration_minutes': 'Duration (min)',
                'status': 'Status',
                'first_seen_at': 'Time',
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No TWAP orders in this window.")

    # ---- TrailBot config ----
    with st.expander("TrailBot v3.0 Config"):
        for key, val in TRAILBOT.items():
            if key not in ('state_file', 'log_file'):
                st.text(f"{key}: {val}")


# ============================================================
# PAGE: POLYMARKET
# ============================================================

def render_polymarket():
    st.header("Polymarket — Where Is The Money Going?")

    # ── Theme selector (or All) ──
    theme_options = ["All Themes"] + list(THEME_TAG_SLUGS.keys())
    selected_theme = st.radio(
        "Theme", theme_options, horizontal=True, label_visibility="collapsed"
    )

    try:
        # ============================================================
        # ROW 1: THEME METRICS
        # ============================================================
        all_markets = get_theme_summary()

        if len(all_markets) == 0:
            st.warning("No Polymarket data available yet.")
            return

        # Assign theme to each market based on event_slug keywords
        def assign_theme(slug):
            slug_lower = str(slug).lower()
            fed_kw = ['fed', 'fomc', 'rate-cut', 'interest-rate', 'inflation',
                       'cpi', 'pce', 'gdp', 'recession', 'employment',
                       'monetary', 'federal-reserve', 'powell', 'jobs-report',
                       'nonfarm', 'payroll', 'consumer-price', 'core-pce']
            trade_kw = ['tariff', 'trade-war', 'trade-deal', 'trade-deficit',
                        'china-tariff', 'blanket-tariff', 'congress-pass']
            geo_kw = ['iran', 'nuclear', 'khamenei', 'israel', 'strike',
                      'war', 'invasion', 'sanctions', 'oil', 'crude',
                      'strait-of-hormuz', 'military', 'geopolit', 'yemen',
                      'hezbollah', 'houthi', 'regime', 'ceasefire']

            for kw in fed_kw:
                if kw in slug_lower:
                    return "Fed & Monetary Policy"
            for kw in trade_kw:
                if kw in slug_lower:
                    return "Trade & Tariffs"
            for kw in geo_kw:
                if kw in slug_lower:
                    return "Geopolitics"
            return "Other"

        all_markets['theme'] = all_markets['event_slug'].apply(assign_theme)

        # Filter by selected theme
        if selected_theme != "All Themes":
            filtered = all_markets[all_markets['theme'] == selected_theme]
        else:
            filtered = all_markets

        # Theme summary cards
        theme_stats = all_markets.groupby('theme').agg(
            markets=('condition_id', 'nunique'),
            vol_24h=('volume_24h', 'sum'),
            total_vol=('total_volume', 'sum'),
            liquidity=('liquidity', 'sum'),
        ).reset_index().sort_values('vol_24h', ascending=False)

        cols = st.columns(len(theme_stats))
        for i, (_, row) in enumerate(theme_stats.iterrows()):
            with cols[i]:
                label = row['theme']
                if row['theme'] == "Fed & Monetary Policy":
                    label = "🏦 Fed"
                elif row['theme'] == "Geopolitics":
                    label = "🌍 Geopolitics"
                elif row['theme'] == "Trade & Tariffs":
                    label = "📦 Trade"
                else:
                    label = "📋 Other"

                st.metric(
                    label=label,
                    value=f"${row['vol_24h']:,.0f}",
                    delta=f"{int(row['markets'])} markets",
                )

        st.divider()

        # ============================================================
        # ROW 1.5: CONSENSUS — What does the market believe?
        # ============================================================
        st.subheader("🎯 Market Consensus — Where Is The Money?")

        consensus = get_consensus(top_n=200, min_volume=1000)

        if len(consensus) > 0:
            # Filter by theme
            if selected_theme != "All Themes":
                consensus['theme'] = consensus['event_slug'].apply(assign_theme)
                consensus = consensus[consensus['theme'] == selected_theme]

            if len(consensus) > 0:
                display_cons = consensus.head(20).copy()

                # Format columns
                display_cons['prob_display'] = display_cons['yes_prob'].apply(
                    lambda x: f"{'🟢' if x >= 50 else '🔴'} {x:.0f}%"
                )
                display_cons['vol_display'] = display_cons['volume_24h'].apply(
                    lambda x: f"${x:,.0f}"
                )
                display_cons['question_short'] = display_cons['question'].str[:75]

                st.dataframe(
                    display_cons[['prob_display', 'question_short', 'vol_display']].rename(
                        columns={
                            'prob_display': 'Prob',
                            'question_short': 'Market',
                            'vol_display': '24h Volume',
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                    height=min(len(display_cons) * 38 + 40, 700),
                )
            else:
                st.info("No consensus data for this theme.")

        st.divider()

        # ============================================================
        # ROW 2: MONEY FLOW — Top events by volume
        # ============================================================
        st.subheader("💰 Money Flow — Top Events (24h Volume)")

        vol_events = get_volume_by_event(top_n=20)
        if len(vol_events) > 0:
            # Filter by theme if selected
            if selected_theme != "All Themes":
                vol_events['theme'] = vol_events['event_slug'].apply(assign_theme)
                vol_events = vol_events[vol_events['theme'] == selected_theme]

            if len(vol_events) > 0:
                # Bar chart
                vol_chart = alt.Chart(vol_events.head(12)).mark_bar(
                    cornerRadiusTopRight=4,
                    cornerRadiusBottomRight=4,
                ).encode(
                    x=alt.X('total_24h_vol:Q', title='24h Volume ($)',
                             axis=alt.Axis(format='~s')),
                    y=alt.Y('event_slug:N', title=None,
                             sort='-x'),
                    color=alt.value('#4ecdc4'),
                    tooltip=[
                        alt.Tooltip('event_slug:N', title='Event'),
                        alt.Tooltip('total_24h_vol:Q', title='24h Vol', format='$,.0f'),
                        alt.Tooltip('market_count:Q', title='Markets'),
                        alt.Tooltip('avg_prob:Q', title='Avg Prob', format='.1f'),
                        alt.Tooltip('total_liquidity:Q', title='Liquidity', format='$,.0f'),
                    ],
                ).properties(height=350)
                st.altair_chart(vol_chart, use_container_width=True)
            else:
                st.info("No volume data for this theme.")

        st.divider()

        # ============================================================
        # ROW 3: BIGGEST MOVERS
        # ============================================================
        st.subheader("⚡ Biggest Movers")

        col1, col2 = st.columns([1, 1])
        with col1:
            mover_hours = st.selectbox(
                "Window", [1, 6, 12, 24, 48],
                index=3, key="pm_mover_hours"
            )
        with col2:
            min_move = st.selectbox(
                "Min move %", [1.0, 2.0, 3.0, 5.0],
                index=2, key="pm_min_move"
            )

        movers = get_top_movers(hours=mover_hours, min_move_pct=min_move)

        if len(movers) > 0:
            # Filter by theme
            if selected_theme != "All Themes":
                movers['theme'] = movers['event_slug'].apply(assign_theme)
                movers = movers[movers['theme'] == selected_theme]

            if len(movers) > 0:
                # Format for display
                display_movers = movers.head(15).copy()
                display_movers['direction'] = display_movers['move_pct'].apply(
                    lambda x: '🟢' if x > 0 else '🔴'
                )
                display_movers['move_display'] = display_movers['move_pct'].apply(
                    lambda x: f"{x:+.1f}%"
                )
                display_movers['prob_display'] = display_movers.apply(
                    lambda r: f"{r['old_prob']:.0f}% → {r['current_prob']:.0f}%",
                    axis=1
                )
                display_movers['vol_display'] = display_movers['volume_24h'].apply(
                    lambda x: f"${x:,.0f}" if pd.notna(x) else "-"
                )
                display_movers['question_short'] = display_movers['question'].str[:70]

                st.dataframe(
                    display_movers[['direction', 'question_short', 'move_display',
                                    'prob_display', 'vol_display']].rename(columns={
                        'direction': '',
                        'question_short': 'Market',
                        'move_display': 'Move',
                        'prob_display': 'Probability',
                        'vol_display': '24h Vol',
                    }),
                    use_container_width=True,
                    hide_index=True,
                    height=min(len(display_movers) * 38 + 40, 600),
                )

                # Probability timeline for top movers
                top_ids = movers.head(5)['condition_id'].tolist()
                if top_ids:
                    st.caption(f"Probability timeline — top {len(top_ids)} movers ({mover_hours}h)")
                    timeline = get_prob_timeline(top_ids, hours=mover_hours)
                    if len(timeline) > 0:
                        # Shorten question for legend
                        timeline['label'] = timeline['question'].str[:45]
                        prob_chart = alt.Chart(timeline).mark_line(
                            strokeWidth=2
                        ).encode(
                            x=alt.X('snapshot_time:T', title=None),
                            y=alt.Y('yes_prob:Q', title='Probability (%)',
                                     scale=alt.Scale(zero=False)),
                            color=alt.Color('label:N', title='Market',
                                            legend=alt.Legend(orient='bottom')),
                            tooltip=[
                                alt.Tooltip('label:N', title='Market'),
                                alt.Tooltip('yes_prob:Q', title='Prob', format='.1f'),
                                alt.Tooltip('snapshot_time:T', title='Time'),
                            ],
                        ).properties(height=300)
                        st.altair_chart(prob_chart, use_container_width=True)
            else:
                st.info(f"No moves >{min_move}% in this theme.")
        else:
            st.info(f"No moves >{min_move}% in the last {mover_hours}h.")

        st.divider()

        # ============================================================
        # ROW 4: EVENT DRILLDOWN
        # ============================================================
        st.subheader("🔍 Event Drilldown")

        events_df = get_event_summary()
        if len(events_df) > 0:
            # Filter by theme
            if selected_theme != "All Themes":
                events_df['theme'] = events_df['event_slug'].apply(assign_theme)
                events_df = events_df[events_df['theme'] == selected_theme]

            if len(events_df) > 0:
                # Build display label: slug + volume hint
                events_df['label'] = events_df.apply(
                    lambda r: f"{r['event_slug']}  (${r['total_24h_vol']:,.0f} vol)",
                    axis=1
                )
                event_labels = events_df['label'].tolist()
                event_slugs = events_df['event_slug'].tolist()

                selected_label = st.selectbox(
                    "Select event", event_labels, key="pm_event_select"
                )
                idx = event_labels.index(selected_label)
                selected_slug = event_slugs[idx]

                markets = get_markets_by_event(selected_slug)
                if len(markets) > 0:
                    markets_display = markets.copy()
                    markets_display['prob_bar'] = markets_display['yes_prob'].apply(
                        lambda x: f"{x:.0f}%"
                    )
                    st.dataframe(
                        markets_display[['question', 'yes_prob', 'volume_24h',
                                         'total_volume', 'change_1d']].rename(columns={
                            'question': 'Market',
                            'yes_prob': 'Prob %',
                            'volume_24h': '24h Vol',
                            'total_volume': 'Total Vol',
                            'change_1d': '1d Δ',
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )

                    # Chart individual market
                    market_options = markets['question'].tolist()
                    market_cids = markets['condition_id'].tolist()
                    selected_market = st.selectbox(
                        "Chart market", market_options, key="pm_market_chart"
                    )
                    midx = market_options.index(selected_market)
                    selected_cid = market_cids[midx]

                    chart_hours = st.select_slider(
                        "Lookback", options=[6, 12, 24, 48],
                        value=24, key="pm_chart_hours"
                    )
                    history = get_pm_market_history(selected_cid, hours=chart_hours)
                    if len(history) > 0:
                        price_chart = alt.Chart(history).mark_area(
                            line={'color': '#4ecdc4', 'strokeWidth': 2},
                            color=alt.Gradient(
                                gradient='linear',
                                stops=[
                                    alt.GradientStop(color='rgba(78, 205, 196, 0.3)', offset=0),
                                    alt.GradientStop(color='rgba(78, 205, 196, 0.02)', offset=1),
                                ],
                                x1=1, x2=1, y1=1, y2=0,
                            ),
                        ).encode(
                            x=alt.X('snapshot_time:T', title=None),
                            y=alt.Y('yes_prob:Q', title='Probability (%)',
                                     scale=alt.Scale(zero=False)),
                            tooltip=[
                                alt.Tooltip('yes_prob:Q', title='Prob', format='.1f'),
                                alt.Tooltip('snapshot_time:T', title='Time'),
                            ],
                        ).properties(height=250)
                        st.altair_chart(price_chart, use_container_width=True)
                    else:
                        st.info("No history data for this market.")

    except Exception as e:
        st.error(f"Polymarket error: {e}")
        import traceback
        st.code(traceback.format_exc())

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