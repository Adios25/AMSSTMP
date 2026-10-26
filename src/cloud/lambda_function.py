import json
import logging
from typing import List, Dict, Any
try:
    from src.cloud.sentiment_engine import DualSentimentEngine
    from src.cloud.db_manager import DatabaseManager
    from src.cloud.alert_manager import AlertManager
except ImportError:
    from sentiment_engine import DualSentimentEngine
    from db_manager import DatabaseManager
    from alert_manager import AlertManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Initialize singletons (reused across Lambda warm invocations)
sentiment_engine = DualSentimentEngine()
db_manager = DatabaseManager()
alert_manager = AlertManager(spike_threshold=0.60)

def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    Serverless handler: Processes a batch of feed items, calculates sentiment,
    persists records to PostgreSQL, and emits SNS alerts for anomalies.
    """
    # Accept batch either as list under 'records' key or direct array
    raw_records = event.get("records", event if isinstance(event, list) else [event])
    
    if not raw_records:
        return {"statusCode": 200, "body": json.dumps({"processed": 0})}

    processed_records: List[Dict[str, Any]] = []
    alerts_triggered: int = 0

    # 1. NLP Sentiment Inference & Enrichment
    for item in raw_records:
        content_type = item.get("content_type", "informal_social")
        clean_text = item.get("clean_text", "")
        
        sentiment_scores = sentiment_engine.process_item(content_type, clean_text)
        
        enriched_record = {
            **item,
            **sentiment_scores
        }
        processed_records.append(enriched_record)

    # 2. Bulk Persist to PostgreSQL Fact Table FIRST (so FK exists)
    persisted_count = db_manager.save_sentiment_batch(processed_records)

    # 3. Anomaly & Spike Evaluation AFTER persistence
    for enriched_record in processed_records:
        alert_type = alert_manager.evaluate_and_notify(enriched_record)
        if alert_type:
            alerts_triggered += 1
            db_manager.log_alert(
                message_id=enriched_record["id"],
                ticker=enriched_record["ticker"],
                alert_type=alert_type,
                score=enriched_record["composite_score"],
                target=alert_manager.topic_arn or "LOCAL_MOCK_SNS"
            )

    response_summary = {
        "received": len(raw_records),
        "persisted": persisted_count,
        "alerts_triggered": alerts_triggered
    }
    logger.info(f"Execution complete: {response_summary}")

    return {
        "statusCode": 200,
        "body": json.dumps(response_summary)
    }

if __name__ == "__main__":
    # Local dry-run test
    sample_event = {
        "records": [
            {
                "id": "test_hash_bullish_001",
                "ticker": "AAPL",
                "source": "reddit",
                "content_type": "informal_social",
                "raw_text": "$AAPL calls printing! Massive breakout inbound 🚀🚀🚀",
                "clean_text": "$AAPL calls printing! Massive breakout inbound 🚀🚀🚀",
                "url": "https://example.com/test",
                "published_at": "2026-09-25T10:00:00Z"
            },
            {
                "id": "test_hash_bearish_002",
                "ticker": "TSLA",
                "source": "finnhub",
                "content_type": "formal_news",
                "raw_text": "Tesla faces regulatory probe and product recall amid severe revenue contraction.",
                "clean_text": "Tesla faces regulatory probe and product recall amid severe revenue contraction.",
                "url": "https://example.com/test2",
                "published_at": "2026-09-25T10:05:00Z"
            }
        ]
    }
    result = lambda_handler(sample_event)
    print("Test Result:", result)
