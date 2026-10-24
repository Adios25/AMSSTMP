import re
import hashlib
from typing import List

def clean_market_text(text: str) -> str:
    """
    Sanitizes raw text:
    - Removes HTML tags
    - Strips URL links
    - Normalizes redundant whitespace
    - Preserves emojis, cashtags ($AAPL), and ALL-CAPS intensity for VADER
    """
    if not text:
        return ""
    # 1. Remove HTML tags
    cleaned = re.sub(r'<[^>]+>', ' ', text)
    # 2. Remove URLs
    cleaned = re.sub(r'https?://\S+|www\.\S+', ' ', cleaned)
    # 3. Collapse multiple whitespaces and newlines into single spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def extract_tickers(text: str) -> List[str]:
    """
    Extracts stock tickers preceded by a dollar sign (e.g., $TSLA, $AAPL).
    """
    matches = re.findall(r'\$([A-Z]{1,5})\b', text)
    return list(set(matches))

def generate_fingerprint(source: str, published_at_iso: str, raw_text: str) -> str:
    """
    Generates a deterministic SHA-256 hash for deduplication and idempotency.
    """
    payload = f"{source.strip().lower()}|{published_at_iso.strip()}|{raw_text.strip()}"
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()
