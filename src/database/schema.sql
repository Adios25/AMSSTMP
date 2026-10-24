-- =============================================================================
-- Autonomous Market & Social Sentiment Trend Monitoring Pipeline (AMSSTMP)
-- PostgreSQL DDL Schema
-- =============================================================================

-- 1. Dimension: Tickers Reference Table
CREATE TABLE IF NOT EXISTS dim_tickers (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    sector VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Seed core tracking universe
INSERT INTO dim_tickers (ticker, company_name, sector) VALUES
    ('AAPL', 'Apple Inc.', 'Technology'),
    ('TSLA', 'Tesla Inc.', 'Automotive / Tech'),
    ('NVDA', 'NVIDIA Corporation', 'Semiconductors'),
    ('MSFT', 'Microsoft Corporation', 'Technology')
ON CONFLICT (ticker) DO NOTHING;

-- 2. Fact: Processed Sentiment Metrics
CREATE TABLE IF NOT EXISTS fact_sentiment_metrics (
    id VARCHAR(64) PRIMARY KEY, -- Deterministic SHA-256 fingerprint
    ticker VARCHAR(10) NOT NULL REFERENCES dim_tickers(ticker),
    source VARCHAR(20) NOT NULL,
    content_type VARCHAR(20) NOT NULL,
    clean_text TEXT NOT NULL,
    url TEXT,
    vader_compound NUMERIC(5, 4) CHECK (vader_compound BETWEEN -1.0000 AND 1.0000),
    finbert_positive NUMERIC(5, 4) CHECK (finbert_positive BETWEEN 0.0000 AND 1.0000),
    finbert_negative NUMERIC(5, 4) CHECK (finbert_negative BETWEEN 0.0000 AND 1.0000),
    finbert_neutral NUMERIC(5, 4) CHECK (finbert_neutral BETWEEN 0.0000 AND 1.0000),
    composite_score NUMERIC(5, 4) NOT NULL CHECK (composite_score BETWEEN -1.0000 AND 1.0000),
    primary_sentiment VARCHAR(10) NOT NULL CHECK (primary_sentiment IN ('BULLISH', 'BEARISH', 'NEUTRAL')),
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- High-performance composite indexes for real-time analytics & window queries
CREATE INDEX IF NOT EXISTS idx_sentiment_ticker_time 
    ON fact_sentiment_metrics (ticker, published_at DESC);

CREATE INDEX IF NOT EXISTS idx_sentiment_composite 
    ON fact_sentiment_metrics (composite_score);

-- 3. Fact: Sentiment Divergence & Spike Alerts
CREATE TABLE IF NOT EXISTS fact_sentiment_alerts (
    alert_id SERIAL PRIMARY KEY,
    message_id VARCHAR(64) REFERENCES fact_sentiment_metrics(id),
    ticker VARCHAR(10) NOT NULL REFERENCES dim_tickers(ticker),
    alert_type VARCHAR(30) NOT NULL, -- 'SPIKE_BULLISH', 'SPIKE_BEARISH', 'DIVERGENCE'
    score NUMERIC(5, 4) NOT NULL,
    notification_target VARCHAR(100), -- SNS Topic ARN or Email
    notification_status VARCHAR(20) DEFAULT 'SENT',
    triggered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_ticker_time 
    ON fact_sentiment_alerts (ticker, triggered_at DESC);
