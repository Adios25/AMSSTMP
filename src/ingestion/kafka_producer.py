import json
import time
import logging
from typing import List
from kafka import KafkaProducer
from src.ingestion.schemas import RawFeedItem
from src.ingestion.fetchers.mock_fetcher import MockFeedFetcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class MarketStreamProducer:
    """
    Kafka Producer for ingesting market news and social streams with partition keying.
    """
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "market-raw-feed"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v, default=str).encode("utf-8"),
            acks="all",            # Full ISR durability
            retries=3,             # Retry transient network issues
            max_in_flight_requests_per_connection=1 # Prevent out-of-order retransmissions
        )
        logger.info(f"Kafka Producer connected to {bootstrap_servers}, targeting topic '{topic}'.")

    def publish_item(self, item: RawFeedItem):
        """
        Publishes a single validated item using ticker as the partition key.
        """
        key = item.ticker
        # Use Pydantic's model_dump_json() to guarantee schema compliance
        value_json = item.model_dump_json()
        
        future = self.producer.send(self.topic, key=key, value=value_json)
        # Flush to confirm receipt and extract partition/offset metadata
        record_metadata = future.get(timeout=10)
        logger.info(
            f"Published [{item.content_type}] for ${item.ticker} -> "
            f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
        )
        return record_metadata

    def close(self):
        self.producer.flush()
        self.producer.close()

def run_ingestion_loop(tickers: List[str], iterations: int = 3, interval_sec: int = 5):
    """
    Main driver loop demonstrating periodic feed extraction and streaming.
    """
    producer = MarketStreamProducer()
    fetcher = MockFeedFetcher()

    logger.info(f"Starting ingestion loop for tickers: {tickers}")
    try:
        for cycle in range(1, iterations + 1):
            logger.info(f"--- Ingestion Cycle {cycle}/{iterations} ---")
            for ticker in tickers:
                items = fetcher.fetch(ticker=ticker, limit=2)
                for item in items:
                    producer.publish_item(item)
            if cycle < iterations:
                time.sleep(interval_sec)
    except KeyboardInterrupt:
        logger.info("Ingestion loop interrupted by user.")
    finally:
        producer.close()
        logger.info("Kafka Producer closed successfully.")

if __name__ == "__main__":
    target_tickers = ["AAPL", "TSLA", "NVDA", "MSFT"]
    run_ingestion_loop(tickers=target_tickers, iterations=2, interval_sec=3)
