import pytest
from datetime import datetime, timezone
from pydantic import ValidationError
from src.ingestion.schemas import RawFeedItem, SourceType, ContentType
from src.ingestion.text_cleaner import clean_market_text, generate_fingerprint, extract_tickers

def test_clean_market_text():
    raw_input = "Check out $AAPL earnings at <a href='http://example.com'>Link</a>! Extremely BULLISH 🚀🚀"
    cleaned = clean_market_text(raw_input)
    assert "<a" not in cleaned
    assert "http://example.com" not in cleaned
    assert "🚀🚀" in cleaned
    assert "BULLISH" in cleaned

def test_extract_tickers():
    text = "Buying $TSLA and $NVDA today, watching $AAPL closely."
    tickers = extract_tickers(text)
    assert set(tickers) == {"TSLA", "NVDA", "AAPL"}

def test_deterministic_fingerprint():
    h1 = generate_fingerprint("finnhub", "2026-09-18T10:00:00Z", "Apple releases new chip")
    h2 = generate_fingerprint("finnhub", "2026-09-18T10:00:00Z", "Apple releases new chip")
    h3 = generate_fingerprint("finnhub", "2026-09-18T10:00:01Z", "Apple releases new chip")
    assert h1 == h2
    assert h1 != h3

def test_raw_feed_item_validation():
    item = RawFeedItem(
        id="test_hash_123",
        source=SourceType.MOCK,
        content_type=ContentType.FORMAL_NEWS,
        ticker="AAPL",
        raw_text="Apple reports record revenue.",
        clean_text="Apple reports record revenue.",
        published_at=datetime.now(timezone.utc)
    )
    assert item.ticker == "AAPL"
    assert item.ingested_at is not None

def test_raw_feed_item_invalid_raises_error():
    with pytest.raises(ValidationError):
        # Missing required fields: id, ticker, clean_text, etc.
        RawFeedItem(
            source="invalid_source",
            raw_text="Missing fields"
        )
