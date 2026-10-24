import time
import requests
from typing import List
from datetime import datetime, timezone
from src.ingestion.schemas import RawFeedItem, SourceType, ContentType
from src.ingestion.text_cleaner import clean_market_text, generate_fingerprint
from src.ingestion.fetchers.base_fetcher import BaseFetcher

class FinnhubNewsFetcher(BaseFetcher):
    """
    Fetches real-time company news from Finnhub Financial API.
    Free tier allows 60 calls/minute.
    """
    BASE_URL = "https://finnhub.io/api/v1/company-news"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("FINNHUB_API_KEY")

    def fetch(self, ticker: str, limit: int = 5) -> List[RawFeedItem]:
        ticker = ticker.upper()
        if not self.api_key:
            return []

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        params = {
            "symbol": ticker,
            "from": today,
            "to": today,
            "token": self.api_key
        }

        try:
            res = requests.get(self.BASE_URL, params=params, timeout=5)
            if res.status_code != 200:
                return []
            articles = res.json()[:limit]
            items: List[RawFeedItem] = []

            for art in articles:
                headline = art.get("headline", "")
                summary = art.get("summary", "")
                full_text = f"{headline}. {summary}".strip()
                if not full_text:
                    continue

                pub_time = datetime.fromtimestamp(art.get("datetime", time.time()), tz=timezone.utc)
                clean = clean_market_text(full_text)
                fingerprint = generate_fingerprint(SourceType.FINNHUB.value, pub_time.isoformat(), full_text)

                items.append(RawFeedItem(
                    id=fingerprint,
                    source=SourceType.FINNHUB,
                    content_type=ContentType.FORMAL_NEWS,
                    ticker=ticker,
                    raw_text=full_text,
                    clean_text=clean,
                    author=art.get("source", "Finnhub"),
                    url=art.get("url"),
                    published_at=pub_time
                ))
            return items
        except Exception:
            return []
