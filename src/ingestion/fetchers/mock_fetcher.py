import random
from datetime import datetime, timezone, timedelta
from typing import List
from src.ingestion.schemas import RawFeedItem, SourceType, ContentType
from src.ingestion.text_cleaner import clean_market_text, generate_fingerprint
from src.ingestion.fetchers.base_fetcher import BaseFetcher

MOCK_FORMAL_HEADLINES = [
    "{ticker} reports Q3 earnings beating Wall Street consensus, operating margin expands by 4.2%.",
    "SEC filing reveals major institutional rebalancing in {ticker} shares following strategic pivot.",
    "{ticker} announces restructuring plan and temporary guidance reduction amidst supply chain headwinds.",
    "Analyst upgrade: {ticker} price target raised following patent approval in enterprise cloud division."
]

MOCK_INFORMAL_POSTS = [
    "Loaded up on ${ticker} calls this morning! Moon mission inbound 🚀🚀🚀 #YOLO",
    "${ticker} looking extremely weak on the 4H chart. Dumping my bags before market close 📉🤮",
    "Can someone explain why ${ticker} is dumping after beating earnings? Market makes zero sense.",
    "${ticker} diamond hands holding strong through this dip! 💎🙌"
]

class MockFeedFetcher(BaseFetcher):
    """
    Generates realistic synthetic financial news and social sentiment feeds
    for local development and pipeline benchmarking.
    """
    def fetch(self, ticker: str, limit: int = 4) -> List[RawFeedItem]:
        ticker = ticker.upper()
        items: List[RawFeedItem] = []
        now = datetime.now(timezone.utc)

        # 1. Generate Formal News items
        for i in range(limit // 2):
            raw = random.choice(MOCK_FORMAL_HEADLINES).format(ticker=ticker)
            pub_time = now - timedelta(minutes=random.randint(1, 120))
            clean = clean_market_text(raw)
            fingerprint = generate_fingerprint(SourceType.MOCK.value, pub_time.isoformat(), raw)
            
            items.append(RawFeedItem(
                id=fingerprint,
                source=SourceType.MOCK,
                content_type=ContentType.FORMAL_NEWS,
                ticker=ticker,
                raw_text=raw,
                clean_text=clean,
                author="Mock Financial Wire",
                url=f"https://example.com/news/{ticker.lower()}/{fingerprint[:8]}",
                published_at=pub_time
            ))

        # 2. Generate Informal Social items
        for i in range(limit // 2):
            raw = random.choice(MOCK_INFORMAL_POSTS).format(ticker=ticker)
            pub_time = now - timedelta(minutes=random.randint(1, 60))
            clean = clean_market_text(raw)
            fingerprint = generate_fingerprint(SourceType.MOCK.value, pub_time.isoformat(), raw)

            items.append(RawFeedItem(
                id=fingerprint,
                source=SourceType.MOCK,
                content_type=ContentType.INFORMAL_SOCIAL,
                ticker=ticker,
                raw_text=raw,
                clean_text=clean,
                author=f"user_{random.randint(100, 999)}",
                url=f"https://example.com/social/{ticker.lower()}/{fingerprint[:8]}",
                published_at=pub_time
            ))

        return items
