-- =============================================================================
-- Autonomous Market & Social Sentiment Trend Monitoring Pipeline (AMSSTMP)
-- Analytical SQL Queries (Phase 6: Backtesting, Aggregations, & Divergence)
-- =============================================================================

-- 1. Rolling Hourly Sentiment Moving Average per Ticker
-- Calculates rolling average composite score, total volume, and sentiment distribution
SELECT 
    ticker,
    DATE_TRUNC('hour', published_at) AS time_window,
    COUNT(*) AS total_signals,
    ROUND(AVG(composite_score), 4) AS avg_composite_score,
    ROUND(AVG(vader_compound), 4) AS avg_vader_score,
    COUNT(CASE WHEN primary_sentiment = 'BULLISH' THEN 1 END) AS bullish_count,
    COUNT(CASE WHEN primary_sentiment = 'BEARISH' THEN 1 END) AS bearish_count,
    COUNT(CASE WHEN primary_sentiment = 'NEUTRAL' THEN 1 END) AS neutral_count
FROM fact_sentiment_metrics
GROUP BY ticker, DATE_TRUNC('hour', published_at)
ORDER BY time_window DESC, total_signals DESC;

-- 2. Sentiment Divergence Query
-- Identifies tickers where Retail Social Sentiment diverges sharply from Institutional News Sentiment
WITH channel_summary AS (
    SELECT 
        ticker,
        content_type,
        COUNT(*) AS count_signals,
        AVG(composite_score) AS channel_sentiment
    FROM fact_sentiment_metrics
    WHERE published_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    GROUP BY ticker, content_type
)
SELECT 
    s.ticker,
    ROUND(s.channel_sentiment, 4) AS social_sentiment,
    ROUND(n.channel_sentiment, 4) AS news_sentiment,
    ROUND(ABS(s.channel_sentiment - n.channel_sentiment), 4) AS divergence_delta,
    CASE 
        WHEN s.channel_sentiment > 0.2 AND n.channel_sentiment < -0.2 THEN 'RETAIL_BULL_NEWS_BEAR'
        WHEN s.channel_sentiment < -0.2 AND n.channel_sentiment > 0.2 THEN 'RETAIL_BEAR_NEWS_BULL'
        ELSE 'ALIGNED_OR_MILD'
    END AS divergence_regime
FROM channel_summary s
JOIN channel_summary n 
    ON s.ticker = n.ticker 
    AND s.content_type = 'informal_social' 
    AND n.content_type = 'formal_news'
ORDER BY divergence_delta DESC;

-- 3. Top Active Outliers & Anomalies
-- Retrieves items that triggered extreme sentiment thresholds
SELECT 
    a.alert_id,
    a.ticker,
    a.alert_type,
    a.score,
    m.clean_text,
    m.source,
    a.triggered_at
FROM fact_sentiment_alerts a
JOIN fact_sentiment_metrics m ON a.message_id = m.id
ORDER BY a.triggered_at DESC
LIMIT 20;
