from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict

class SourceType(str, Enum):
    FINNHUB = "finnhub"
    REDDIT = "reddit"
    STOCKTWITS = "stocktwits"
    NEWSAPI = "newsapi"
    MOCK = "mock"

class ContentType(str, Enum):
    FORMAL_NEWS = "formal_news"       # Routed to FinBERT (formal institutional tone)
    INFORMAL_SOCIAL = "informal_social" # Routed to VADER (slang, emojis, retail hype)

class RawFeedItem(BaseModel):
    """
    Data Contract for all incoming market sentiment feed items.
    Every producer MUST conform to this schema before streaming to Kafka.
    """
    model_config = ConfigDict(use_enum_values=True)

    id: str = Field(description="Deterministic SHA-256 hash fingerprint for deduplication")
    source: SourceType = Field(description="Origin data source of the item")
    content_type: ContentType = Field(description="Category of the text content")
    ticker: str = Field(min_length=1, max_length=10, description="Uppercase stock ticker symbol (e.g., AAPL)")
    raw_text: str = Field(min_length=1, description="Original unmodified text")
    clean_text: str = Field(min_length=1, description="Sanitized text for NLP processing")
    author: Optional[str] = Field(default=None, description="Author or publisher if available")
    url: Optional[str] = Field(default=None, description="Original source link")
    published_at: datetime = Field(description="Timestamp when source published the item")
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when the pipeline ingested the record"
    )
