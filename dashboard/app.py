"""
Streamlit Dashboard - E-Commerce Clickstream Analytics
=======================================================
Dashboard interaktif real-time yang menampilkan:
  1. Real-time metrics (total events, rate, unique users, suspicious count)
  2. Trending products (windowed aggregation)
  3. Event type distribution
  4. Suspicious activity alerts
  5. Live event feed
  6. Event timeline

Auto-refresh setiap 5 detik.
Mendukung Dark Mode / Light Mode.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import os
from datetime import datetime, timedelta

# ============================================
# Page Configuration
# ============================================
st.set_page_config(
    page_title="🛒 E-Commerce Clickstream Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================
# Database Connection
# ============================================
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432"),
    "dbname": os.environ.get("DB_NAME", "clickstream_db"),
    "user": os.environ.get("DB_USER", "admin"),
    "password": os.environ.get("DB_PASSWORD", "admin123"),
}


@st.cache_resource
def get_connection():
    """Get database connection (cached)."""
    return psycopg2.connect(**DB_CONFIG)


def run_query(query, params=None):
    """Execute query and return DataFrame."""
    try:
        conn = get_connection()
        # Check if connection is alive
        try:
            conn.cursor().execute("SELECT 1")
            conn.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            st.cache_resource.clear()
            conn = get_connection()
        
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        st.cache_resource.clear()
        return pd.DataFrame()


# ============================================
# Theme Configuration
# ============================================
def get_theme_config(theme):
    """Get Plotly theme configuration."""
    if theme == "🌙 Dark Mode":
        return {
            "template": "plotly_dark",
            "bg_color": "#0e1117",
            "card_bg": "#1e2130",
            "text_color": "#fafafa",
            "accent_1": "#00d4aa",  # Teal
            "accent_2": "#667eea",  # Purple-blue
            "accent_3": "#f093fb",  # Pink
            "accent_4": "#fbbf24",  # Amber
            "gradient": ["#667eea", "#764ba2", "#f093fb"],
            "chart_colors": [
                "#00d4aa", "#667eea", "#f093fb", "#fbbf24",
                "#ef4444", "#06b6d4", "#8b5cf6", "#10b981",
            ],
        }
    else:
        return {
            "template": "plotly_white",
            "bg_color": "#ffffff",
            "card_bg": "#f8f9fa",
            "text_color": "#1a1a2e",
            "accent_1": "#0ea5e9",  # Sky blue
            "accent_2": "#6366f1",  # Indigo
            "accent_3": "#ec4899",  # Pink
            "accent_4": "#f59e0b",  # Amber
            "gradient": ["#6366f1", "#8b5cf6", "#ec4899"],
            "chart_colors": [
                "#0ea5e9", "#6366f1", "#ec4899", "#f59e0b",
                "#ef4444", "#14b8a6", "#8b5cf6", "#22c55e",
            ],
        }


# ============================================
# Custom CSS
# ============================================
def apply_custom_css(theme):
    """Apply custom CSS based on theme."""
    tc = get_theme_config(theme)

    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        /* Global */
        .stApp {{
            font-family: 'Inter', sans-serif;
        }}

        /* Metric cards */
        .metric-card {{
            background: linear-gradient(135deg, {tc['card_bg']}, {tc['card_bg']}ee);
            border: 1px solid {tc['accent_1']}30;
            border-radius: 16px;
            padding: 20px 24px;
            text-align: center;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        .metric-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 8px 25px {tc['accent_1']}20;
        }}
        .metric-value {{
            font-size: 2.2rem;
            font-weight: 700;
            background: linear-gradient(135deg, {tc['accent_1']}, {tc['accent_2']});
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 4px;
        }}
        .metric-label {{
            font-size: 0.85rem;
            font-weight: 500;
            color: {tc['text_color']}90;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .metric-delta {{
            font-size: 0.75rem;
            color: {tc['accent_1']};
            margin-top: 4px;
        }}

        /* Section headers */
        .section-header {{
            font-size: 1.3rem;
            font-weight: 600;
            margin-bottom: 12px;
            padding-bottom: 8px;
            border-bottom: 2px solid {tc['accent_2']}40;
        }}

        /* Alert badge */
        .alert-badge {{
            display: inline-block;
            background: linear-gradient(135deg, #ef4444, #dc2626);
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            animation: pulse 2s infinite;
        }}
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.7; }}
        }}

        /* Status indicator */
        .status-live {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            color: #22c55e;
            font-weight: 600;
            font-size: 0.85rem;
        }}
        .status-dot {{
            width: 8px;
            height: 8px;
            background: #22c55e;
            border-radius: 50%;
            animation: blink 1.5s infinite;
        }}
        @keyframes blink {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.3; }}
        }}

        /* Divider */
        hr {{
            border: none;
            border-top: 1px solid {tc['accent_2']}20;
            margin: 1.5rem 0;
        }}

        /* Hide Streamlit branding */
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
    </style>
    """, unsafe_allow_html=True)


def metric_card(label, value, delta=None):
    """Render a styled metric card."""
    delta_html = f'<div class="metric-delta">{delta}</div>' if delta else ""
    return f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{label}</div>
        {delta_html}
    </div>
    """


# ============================================
# Data Queries
# ============================================
def get_overview_metrics():
    """Get dashboard overview metrics."""
    query = """
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(CASE WHEN is_suspicious THEN 1 END) as suspicious_count,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
            COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN product_price ELSE 0 END), 0) as total_revenue,
            COUNT(CASE WHEN processed_at >= NOW() - INTERVAL '1 minute' THEN 1 END) as events_last_minute
        FROM processed_events
    """
    return run_query(query)


def get_trending_products(limit=10):
    """Get trending products from windowed aggregation."""
    query = """
        SELECT 
            product_name,
            product_category,
            SUM(view_count) as total_views,
            SUM(unique_users) as total_unique_users,
            MAX(window_end) as last_window
        FROM product_views_per_minute
        WHERE window_start >= NOW() - INTERVAL '10 minutes'
        GROUP BY product_name, product_category
        ORDER BY total_views DESC
        LIMIT %s
    """
    return run_query(query, (limit,))


def get_event_type_distribution():
    """Get event type distribution."""
    query = """
        SELECT 
            event_type,
            COUNT(*) as count
        FROM processed_events
        GROUP BY event_type
        ORDER BY count DESC
    """
    return run_query(query)


def get_suspicious_activities(limit=20):
    """Get recent suspicious activities."""
    query = """
        SELECT 
            user_id,
            user_name,
            event_count,
            reason,
            detected_at
        FROM suspicious_activities
        ORDER BY detected_at DESC
        LIMIT %s
    """
    return run_query(query, (limit,))


def get_live_feed(limit=50):
    """Get latest events."""
    query = """
        SELECT 
            event_id,
            user_id,
            user_name,
            user_city,
            membership,
            event_type,
            product_name,
            product_category,
            product_price,
            device,
            is_suspicious,
            event_timestamp,
            processed_at
        FROM processed_events
        ORDER BY processed_at DESC
        LIMIT %s
    """
    return run_query(query, (limit,))


def get_events_per_minute(minutes=30):
    """Get event counts per minute for timeline."""
    query = """
        SELECT 
            date_trunc('minute', processed_at) as minute,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(CASE WHEN is_suspicious THEN 1 END) as suspicious_count
        FROM processed_events
        WHERE processed_at >= NOW() - INTERVAL '%s minutes'
        GROUP BY minute
        ORDER BY minute ASC
    """
    return run_query(query % minutes)


def get_category_breakdown():
    """Get views by product category."""
    query = """
        SELECT
            product_category,
            COUNT(*) as total_events,
            COUNT(CASE WHEN event_type = 'view_product' THEN 1 END) as views,
            COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) as add_to_cart,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases
        FROM processed_events
        GROUP BY product_category
        ORDER BY total_events DESC
    """
    return run_query(query)


def get_device_distribution():
    """Get event distribution by device type."""
    query = """
        SELECT device, COUNT(*) as count
        FROM processed_events
        GROUP BY device
        ORDER BY count DESC
    """
    return run_query(query)


def get_membership_stats():
    """Get stats by membership tier."""
    query = """
        SELECT
            membership,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as user_count,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases
        FROM processed_events
        GROUP BY membership
        ORDER BY event_count DESC
    """
    return run_query(query)


# ============================================
# Dashboard Layout
# ============================================
def main():
    # Sidebar
    with st.sidebar:
        st.markdown("## ⚙️ Settings")
        theme = st.radio(
            "Theme",
            ["🌙 Dark Mode", "☀️ Light Mode"],
            index=0,
        )
        refresh_rate = st.selectbox(
            "Auto-refresh interval",
            [5, 10, 15, 30],
            index=0,
            format_func=lambda x: f"{x} seconds",
        )
        st.markdown("---")
        st.markdown("### 📖 About")
        st.markdown(
            "Real-time analytics dashboard for e-commerce clickstream data, "
            "powered by **Apache Kafka** and **PostgreSQL**."
        )
        st.markdown("---")
        st.markdown(
            '<div class="status-live">'
            '<span class="status-dot"></span> LIVE - Auto Refresh'
            '</div>',
            unsafe_allow_html=True,
        )

    # Apply theme CSS
    apply_custom_css(theme)
    tc = get_theme_config(theme)

    # Header
    st.markdown(
        """
        <div style="text-align: center; padding: 10px 0 20px 0;">
            <h1 style="margin-bottom: 4px;">🛒 E-Commerce Clickstream Analytics</h1>
            <p style="opacity: 0.7; font-size: 1rem;">
                Real-time streaming dashboard — Apache Kafka Pipeline
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---- ROW 1: Overview Metrics ----
    metrics = get_overview_metrics()
    if not metrics.empty:
        m = metrics.iloc[0]
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.markdown(
                metric_card("Total Events", f"{int(m['total_events']):,}"),
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                metric_card(
                    "Events / Menit",
                    f"{int(m['events_last_minute']):,}",
                    delta="⏱️ Last 60s",
                ),
                unsafe_allow_html=True,
            )
        with col3:
            st.markdown(
                metric_card("Unique Users", f"{int(m['unique_users']):,}"),
                unsafe_allow_html=True,
            )
        with col4:
            st.markdown(
                metric_card("Purchases", f"{int(m['purchases']):,}"),
                unsafe_allow_html=True,
            )
        with col5:
            suspicious_val = int(m["suspicious_count"])
            st.markdown(
                metric_card(
                    "Suspicious",
                    f"{suspicious_val:,}",
                    delta='<span class="alert-badge">⚠️ ALERT</span>' if suspicious_val > 0 else "✅ Clean",
                ),
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ---- ROW 2: Trending Products + Event Distribution ----
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown('<div class="section-header">🔥 Trending Products (10 Menit Terakhir)</div>', unsafe_allow_html=True)
        trending = get_trending_products(10)
        if not trending.empty:
            fig = px.bar(
                trending.sort_values("total_views", ascending=True),
                x="total_views",
                y="product_name",
                color="product_category",
                orientation="h",
                color_discrete_sequence=tc["chart_colors"],
                labels={"total_views": "Total Views", "product_name": "Product", "product_category": "Category"},
            )
            fig.update_layout(
                template=tc["template"],
                height=400,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                yaxis=dict(tickfont=dict(size=11)),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("⏳ Menunggu data aggregasi... (perlu minimal 1 menit)")

    with col_right:
        st.markdown('<div class="section-header">📊 Event Type Distribution</div>', unsafe_allow_html=True)
        event_dist = get_event_type_distribution()
        if not event_dist.empty:
            # Custom label mapping
            label_map = {
                "view_product": "👁️ View",
                "search": "🔍 Search",
                "add_to_cart": "🛒 Add to Cart",
                "purchase": "💳 Purchase",
            }
            event_dist["label"] = event_dist["event_type"].map(
                lambda x: label_map.get(x, x)
            )

            fig = px.pie(
                event_dist,
                values="count",
                names="label",
                color_discrete_sequence=tc["chart_colors"],
                hole=0.45,
            )
            fig.update_layout(
                template=tc["template"],
                height=400,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(font=dict(size=12)),
            )
            fig.update_traces(
                textposition="inside",
                textinfo="percent+value",
                textfont_size=12,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("⏳ Menunggu data...")

    st.markdown("---")

    # ---- ROW 3: Timeline + Category Breakdown ----
    col_left2, col_right2 = st.columns([3, 2])

    with col_left2:
        st.markdown('<div class="section-header">📈 Event Timeline (Per Menit)</div>', unsafe_allow_html=True)
        timeline = get_events_per_minute(30)
        if not timeline.empty:
            def hex_to_rgba(h, a):
                h = h.lstrip('#')
                return f"rgba({int(h[0:2], 16)}, {int(h[2:4], 16)}, {int(h[4:6], 16)}, {a})"

            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Scatter(
                    x=timeline["minute"],
                    y=timeline["event_count"],
                    mode="lines+markers",
                    name="Events",
                    line=dict(color=tc["accent_1"], width=2.5),
                    marker=dict(size=5),
                    fill="tozeroy",
                    fillcolor=hex_to_rgba(tc["accent_1"], 0.15),
                ),
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=timeline["minute"],
                    y=timeline["unique_users"],
                    mode="lines+markers",
                    name="Unique Users",
                    line=dict(color=tc["accent_2"], width=2, dash="dot"),
                    marker=dict(size=4),
                ),
                secondary_y=True,
            )
            # Add suspicious as bar
            if timeline["suspicious_count"].sum() > 0:
                fig.add_trace(
                    go.Bar(
                        x=timeline["minute"],
                        y=timeline["suspicious_count"],
                        name="🚨 Suspicious",
                        marker_color="rgba(239, 68, 68, 0.5)",
                        yaxis="y",
                    ),
                )

            fig.update_layout(
                template=tc["template"],
                height=350,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
            )
            fig.update_yaxes(title_text="Event Count", secondary_y=False)
            fig.update_yaxes(title_text="Unique Users", secondary_y=True)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("⏳ Menunggu data...")

    with col_right2:
        st.markdown('<div class="section-header">🏷️ Category Breakdown</div>', unsafe_allow_html=True)
        categories = get_category_breakdown()
        if not categories.empty:
            fig = px.bar(
                categories,
                x="product_category",
                y=["views", "add_to_cart", "purchases"],
                barmode="stack",
                color_discrete_sequence=[tc["accent_1"], tc["accent_4"], tc["accent_3"]],
                labels={
                    "product_category": "Category",
                    "value": "Count",
                    "variable": "Event Type",
                },
            )
            fig.update_layout(
                template=tc["template"],
                height=350,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1,
                    title=None,
                ),
                xaxis_tickangle=-30,
            )
            # Rename legend entries
            newnames = {"views": "👁️ Views", "add_to_cart": "🛒 Cart", "purchases": "💳 Purchase"}
            fig.for_each_trace(lambda t: t.update(name=newnames.get(t.name, t.name)))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("⏳ Menunggu data...")

    st.markdown("---")

    # ---- ROW 4: Device + Membership Stats ----
    col_dev, col_mem = st.columns(2)

    with col_dev:
        st.markdown('<div class="section-header">📱 Device Distribution</div>', unsafe_allow_html=True)
        devices = get_device_distribution()
        if not devices.empty:
            device_icons = {"mobile": "📱", "desktop": "🖥️", "tablet": "📟"}
            devices["label"] = devices["device"].map(
                lambda x: f"{device_icons.get(x, '❓')} {x.title()}"
            )
            fig = px.pie(
                devices,
                values="count",
                names="label",
                color_discrete_sequence=[tc["accent_1"], tc["accent_2"], tc["accent_4"]],
                hole=0.5,
            )
            fig.update_layout(
                template=tc["template"],
                height=280,
                margin=dict(l=0, r=0, t=10, b=0),
            )
            fig.update_traces(textposition="inside", textinfo="percent+label", textfont_size=11)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("⏳ Menunggu data...")

    with col_mem:
        st.markdown('<div class="section-header">🏅 Membership Tiers</div>', unsafe_allow_html=True)
        membership = get_membership_stats()
        if not membership.empty:
            tier_colors = {
                "platinum": "#E5E4E2",
                "gold": "#FFD700",
                "silver": "#C0C0C0",
                "bronze": "#CD7F32",
                "none": "#808080",
            }
            membership["color"] = membership["membership"].map(
                lambda x: tier_colors.get(x, "#808080")
            )
            fig = px.bar(
                membership,
                x="membership",
                y="event_count",
                color="membership",
                color_discrete_map=tier_colors,
                text="user_count",
                labels={"membership": "Tier", "event_count": "Total Events", "user_count": "Users"},
            )
            fig.update_traces(texttemplate="%{text} users", textposition="outside")
            fig.update_layout(
                template=tc["template"],
                height=280,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                xaxis_title=None,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("⏳ Menunggu data...")

    st.markdown("---")

    # ---- ROW 5: Suspicious Activities ----
    st.markdown('<div class="section-header">🚨 Suspicious Activity Alerts</div>', unsafe_allow_html=True)
    suspicious = get_suspicious_activities(20)
    if not suspicious.empty:
        # Style the dataframe
        def highlight_suspicious(row):
            return ["background-color: #ef444420"] * len(row)

        styled_df = suspicious[["detected_at", "user_id", "user_name", "event_count", "reason"]].copy()
        styled_df.columns = ["Detected At", "User ID", "Name", "Events", "Reason"]
        st.dataframe(
            styled_df.style.apply(highlight_suspicious, axis=1),
            use_container_width=True,
            height=300,
        )
    else:
        st.success("✅ Tidak ada aktivitas mencurigakan terdeteksi.")

    st.markdown("---")

    # ---- ROW 6: Live Event Feed ----
    st.markdown('<div class="section-header">📋 Live Event Feed (50 Terbaru)</div>', unsafe_allow_html=True)
    feed = get_live_feed(50)
    if not feed.empty:
        display_df = feed[[
            "processed_at", "user_name", "user_city", "event_type",
            "product_name", "product_category", "product_price", "device",
            "membership", "is_suspicious",
        ]].copy()
        display_df.columns = [
            "Time", "User", "City", "Event", "Product",
            "Category", "Price (IDR)", "Device", "Tier", "Suspicious",
        ]
        display_df["Price (IDR)"] = display_df["Price (IDR)"].apply(
            lambda x: f"Rp {x:,.0f}" if pd.notnull(x) else "-"
        )
        display_df["Suspicious"] = display_df["Suspicious"].map(
            {True: "🚨 Yes", False: "✅ No"}
        )

        event_emojis = {
            "view_product": "👁️",
            "search": "🔍",
            "add_to_cart": "🛒",
            "purchase": "💳",
        }
        display_df["Event"] = display_df["Event"].map(
            lambda x: f"{event_emojis.get(x, '❓')} {x}"
        )

        st.dataframe(display_df, use_container_width=True, height=400)
    else:
        st.info("⏳ Menunggu data masuk...")

    # Footer
    st.markdown(
        """
        <div style="text-align: center; padding: 20px 0; opacity: 0.5; font-size: 0.8rem;">
            Built with Apache Kafka • PostgreSQL • Streamlit | 
            Tugas Besar Big Data — Real-Time Data Streaming Pipeline
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---- Auto Refresh ----
    import time as time_mod
    time_mod.sleep(refresh_rate)
    st.rerun()


# ============================================
# Entry Point
# ============================================
if __name__ == "__main__":
    main()
