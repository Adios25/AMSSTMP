import time
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv

from src.ingestion.fetchers.mock_fetcher import MockFeedFetcher
from src.ingestion.kafka_producer import MarketStreamProducer
from src.streaming.cloud_bridge import CloudBridgeWorker
from src.cloud.db_manager import DatabaseManager
from src.cloud.sentiment_engine import DualSentimentEngine

load_dotenv()

st.set_page_config(
    page_title="AMSSTMP - Market Sentiment Dashboard",
    page_icon="📈",
    layout="wide"
)

# Custom Styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        color: #4B5563;
        font-size: 1.05rem;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #3B82F6;
    }
    .flow-box {
        background-color: #EFF6FF;
        border: 1px solid #BFDBFE;
        border-radius: 8px;
        padding: 0.8rem;
        text-align: center;
        font-weight: 600;
        color: #1E40AF;
    }
</style>
""", unsafe_allow_html=True)

# App Header
st.markdown('<div class="main-header">📈 Autonomous Market & Social Sentiment Pipeline</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Real-time Hybrid Streaming • Apache Kafka • Dual NLP (VADER + FinBERT) • Amazon RDS • Amazon SNS</div>', unsafe_allow_html=True)

# Sidebar Configuration
st.sidebar.header("🕹️ Pipeline Control Panel")

db_target = os.getenv("DB_HOST", "127.0.0.1")
is_cloud = "rds.amazonaws.com" in db_target

st.sidebar.markdown(f"**Database Target:** `{'AWS RDS PostgreSQL (Cloud)' if is_cloud else 'Local PostgreSQL (Docker)'}`")
st.sidebar.markdown(f"**Host:** `{db_target}`")

selected_tickers = st.sidebar.multiselect(
    "Select Tickers to Ingest & Monitor",
    options=["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOGL"],
    default=["AAPL", "TSLA", "NVDA", "MSFT"]
)

cycles = st.sidebar.slider("Streaming Cycles", min_value=1, max_value=5, value=2)
interval_sec = st.sidebar.slider("Polling Interval (sec)", min_value=1, max_value=5, value=2)

st.sidebar.divider()

# Interactive Pipeline Runner Function
def run_interactive_pipeline(tickers, num_cycles, interval):
    status_container = st.container()
    
    with status_container:
        st.info("🚀 Starting Streaming Execution...")
        progress_bar = st.progress(0)
        
        # Step 1: Ingestion
        flow_cols = st.columns(4)
        flow_cols[0].markdown('<div class="flow-box">1. Producer Ingesting</div>', unsafe_allow_html=True)
        flow_cols[1].markdown('<div class="flow-box" style="opacity: 0.4;">2. Kafka Buffering</div>', unsafe_allow_html=True)
        flow_cols[2].markdown('<div class="flow-box" style="opacity: 0.4;">3. NLP Inference</div>', unsafe_allow_html=True)
        flow_cols[3].markdown('<div class="flow-box" style="opacity: 0.4;">4. DB & SNS Alert</div>', unsafe_allow_html=True)
        
        fetcher = MockFeedFetcher()
        producer = MarketStreamProducer()
        
        total_items = []
        for c in range(num_cycles):
            for t in tickers:
                items = fetcher.fetch(ticker=t, limit=2)
                for item in items:
                    producer.publish_item(item)
                    total_items.append(item.model_dump())
            time.sleep(interval)
            progress_bar.progress(int(((c + 1) / (num_cycles + 1)) * 50))
            
        producer.close()
        flow_cols[1].markdown('<div class="flow-box">2. Kafka Partitioned ✅</div>', unsafe_allow_html=True)
        
        # Step 2: Bridge & Serverless Execution
        flow_cols[2].markdown('<div class="flow-box">3. NLP Processing...</div>', unsafe_allow_html=True)
        bridge = CloudBridgeWorker(batch_size=4)
        bridge.start_bridge(max_batches=max(2, len(total_items) // 4))
        
        flow_cols[2].markdown('<div class="flow-box">3. Dual NLP Evaluated ✅</div>', unsafe_allow_html=True)
        flow_cols[3].markdown('<div class="flow-box">4. Persisted to RDS & SNS ✅</div>', unsafe_allow_html=True)
        progress_bar.progress(100)
        st.success(f"Successfully processed and stored {len(total_items)} streaming events!")

# Trigger Button
if st.sidebar.button("▶️ Run Streaming Ingestion & Analysis", type="primary"):
    if not selected_tickers:
        st.sidebar.error("Please select at least one ticker.")
    else:
        run_interactive_pipeline(selected_tickers, cycles, interval_sec)

# Real-time Metrics & DB Explorer
db = DatabaseManager()

def load_data():
    try:
        conn = db.get_connection()
        metrics_df = pd.read_sql("""
            SELECT ticker, source, content_type, primary_sentiment, 
                   composite_score, vader_compound, finbert_positive, finbert_negative,
                   clean_text, published_at, processed_at
            FROM fact_sentiment_metrics
            ORDER BY processed_at DESC
            LIMIT 100;
        """, conn)
        
        alerts_df = pd.read_sql("""
            SELECT alert_id, ticker, alert_type, score, notification_status, triggered_at
            FROM fact_sentiment_alerts
            ORDER BY triggered_at DESC
            LIMIT 20;
        """, conn)
        conn.close()
        return metrics_df, alerts_df
    except Exception as e:
        st.error(f"Failed to query database: {e}")
        return pd.DataFrame(), pd.DataFrame()

metrics_df, alerts_df = load_data()

# Live KPI Metric Cards
kpi1, kpi2, kpi3, kpi4 = st.columns(4)
total_records = len(metrics_df)
total_alerts = len(alerts_df)
bullish_pct = (len(metrics_df[metrics_df["primary_sentiment"] == "BULLISH"]) / total_records * 100) if total_records > 0 else 0
avg_score = metrics_df["composite_score"].mean() if total_records > 0 else 0.0

kpi1.metric("📊 Total Stream Records", total_records)
kpi2.metric("🚨 Triggered Alerts", total_alerts)
kpi3.metric("🐂 Bullish Ratio", f"{bullish_pct:.1f}%")
kpi4.metric("📈 Avg Sentiment", f"{avg_score:+.2f}")

st.divider()

# Real-time Visualizations
viz_col1, viz_col2 = st.columns(2)

with viz_col1:
    st.subheader("📊 Sentiment Score by Ticker")
    if not metrics_df.empty:
        fig_bar = px.box(
            metrics_df, 
            x="ticker", 
            y="composite_score", 
            color="ticker",
            points="all",
            title="Composite Score Distribution per Ticker",
            labels={"composite_score": "Sentiment Score (-1.0 to +1.0)", "ticker": "Stock Symbol"}
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No sentiment data available. Click 'Run Streaming Ingestion' in the sidebar.")

with viz_col2:
    st.subheader("🎯 Primary Sentiment Breakdown")
    if not metrics_df.empty:
        fig_pie = px.pie(
            metrics_df, 
            names="primary_sentiment", 
            title="BULLISH vs. BEARISH vs. NEUTRAL Share",
            color="primary_sentiment",
            color_discrete_map={"BULLISH": "#10B981", "BEARISH": "#EF4444", "NEUTRAL": "#6B7280"}
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No sentiment data available.")

# Divergence Analysis Section
st.subheader("⚖️ Retail Social vs. Institutional News Divergence")
if not metrics_df.empty:
    div_summary = metrics_df.groupby(["ticker", "content_type"])["composite_score"].mean().unstack()
    if "informal_social" in div_summary.columns and "formal_news" in div_summary.columns:
        div_summary["divergence_delta"] = (div_summary["informal_social"] - div_summary["formal_news"]).abs()
        st.dataframe(div_summary.style.format("{:+.3f}"), use_container_width=True)
    else:
        st.info("Both formal_news and informal_social streams needed to compute divergence delta.")

# Real-Time Data Tables
tab1, tab2 = st.tabs(["📋 Processed Stream Feeds (fact_sentiment_metrics)", "🚨 Triggered Alerts (fact_sentiment_alerts)"])

with tab1:
    if not metrics_df.empty:
        st.dataframe(
            metrics_df[["ticker", "primary_sentiment", "composite_score", "source", "content_type", "clean_text", "published_at"]],
            use_container_width=True,
            height=300
        )
    else:
        st.write("No records found.")

with tab2:
    if not alerts_df.empty:
        st.dataframe(
            alerts_df[["alert_id", "ticker", "alert_type", "score", "notification_status", "triggered_at"]],
            use_container_width=True,
            height=250
        )
    else:
        st.write("No alerts recorded yet.")

st.caption("AMSSTMP Dashboard • Connected to: " + db_target)
