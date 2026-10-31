import os
import logging
import requests
from typing import Dict, Any, Optional
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

HF_ROUTER_URL = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert"
HF_LEGACY_URL = "https://api-inference.huggingface.co/models/ProsusAI/finbert"

# Domain-specific financial sentiment lexicon
FINANCIAL_LEXICON = {
    # Bullish slang & terms
    "moon": 2.5,
    "mooning": 2.8,
    "breakout": 2.2,
    "printing": 2.0,
    "bullish": 2.5,
    "calls": 1.5,
    "buy": 1.8,
    "long": 1.5,
    "rally": 2.0,
    "beat": 2.0,
    "beating": 2.0,
    "outperform": 2.2,
    "upgrade": 2.0,
    "diamond hands": 2.5,
    "undervalued": 1.8,

    # Bearish slang & terms
    "dump": -2.5,
    "dumping": -2.8,
    "crash": -2.8,
    "crashing": -3.0,
    "bearish": -2.5,
    "puts": -1.5,
    "sell": -1.8,
    "short": -1.5,
    "miss": -2.0,
    "missing": -2.0,
    "downgrade": -2.2,
    "bagholder": -2.2,
    "plunge": -2.5,
    "recession": -2.0,
    "overvalued": -1.8
}

class DualSentimentEngine:
    """
    Dual-engine sentiment analysis framework combining:
    - VADER (enhanced with financial market lexicon)
    - FinBERT (via Hugging Face API for formal news)
    """
    def __init__(self, hf_token: Optional[str] = None):
        self.vader = SentimentIntensityAnalyzer()
        # Inject financial trading vocabulary into VADER's rule engine
        self.vader.lexicon.update(FINANCIAL_LEXICON)
        
        self.hf_token = hf_token or os.getenv("HF_API_TOKEN")
        self.headers = {"Authorization": f"Bearer {self.hf_token}"} if self.hf_token else {}

    def analyze_vader(self, text: str) -> Dict[str, float]:
        """Runs financial-aware VADER on clean text."""
        scores = self.vader.polarity_scores(text)
        return {
            "compound": float(scores["compound"]),
            "pos": float(scores["pos"]),
            "neg": float(scores["neg"]),
            "neu": float(scores["neu"])
        }

    def analyze_finbert(self, text: str) -> Optional[Dict[str, float]]:
        """Queries Hugging Face Inference API for FinBERT with fallback to router."""
        if not self.hf_token or self.hf_token.startswith("hf_your_"):
            return None

        for endpoint in [HF_ROUTER_URL, HF_LEGACY_URL]:
            try:
                response = requests.post(
                    endpoint,
                    headers=self.headers,
                    json={"inputs": text[:512]},
                    timeout=5
                )
                if response.status_code == 200:
                    raw_scores = response.json()
                    if isinstance(raw_scores, list) and len(raw_scores) > 0:
                        inner = raw_scores[0]
                        if isinstance(inner, list):
                            score_list = inner
                        elif isinstance(inner, dict):
                            score_list = raw_scores
                        else:
                            continue
                        score_map = {item["label"].lower(): float(item["score"]) for item in score_list if "label" in item and "score" in item}
                        if score_map:
                            return {
                                "positive": score_map.get("positive", 0.0),
                                "negative": score_map.get("negative", 0.0),
                                "neutral": score_map.get("neutral", 0.0)
                            }
            except Exception as e:
                logger.debug(f"FinBERT endpoint {endpoint} failed ({e}).")
                continue

        return None

    def process_item(self, content_type: str, text: str) -> Dict[str, Any]:
        """Routes text to appropriate model and computes composite score."""
        vader_res = self.analyze_vader(text)
        finbert_res = None

        if content_type == "formal_news":
            finbert_res = self.analyze_finbert(text)

        if finbert_res:
            finbert_score = finbert_res["positive"] - finbert_res["negative"]
            composite = (0.8 * finbert_score) + (0.2 * vader_res["compound"])
        else:
            composite = vader_res["compound"]

        composite = round(max(-1.0, min(1.0, composite)), 4)

        if composite >= 0.15:
            sentiment_label = "BULLISH"
        elif composite <= -0.15:
            sentiment_label = "BEARISH"
        else:
            sentiment_label = "NEUTRAL"

        return {
            "vader_compound": round(vader_res["compound"], 4),
            "finbert_positive": round(finbert_res["positive"], 4) if finbert_res else None,
            "finbert_negative": round(finbert_res["negative"], 4) if finbert_res else None,
            "finbert_neutral": round(finbert_res["neutral"], 4) if finbert_res else None,
            "composite_score": composite,
            "primary_sentiment": sentiment_label
        }
