import time
import threading
import logging
from src.ingestion.kafka_producer import run_ingestion_loop
from src.streaming.cloud_bridge import CloudBridgeWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s")
logger = logging.getLogger("PipelineRunner")

def run_consumer_thread():
    """Runs the consumer bridge in a background worker thread."""
    bridge = CloudBridgeWorker(batch_size=4)
    # Process batches continuously
    bridge.start_bridge(max_records=16, max_idle_seconds=30)

def run_end_to_end_pipeline():
    """
    Launches producer and consumer concurrently to demonstrate
    real-time streaming, micro-batching, NLP scoring, and DB persistence.
    """
    logger.info("=== Starting Autonomous Market Sentiment Pipeline (E2E) ===")

    # 1. Start Consumer Bridge in background thread
    consumer_thread = threading.Thread(target=run_consumer_thread, name="ConsumerBridge")
    consumer_thread.start()
    time.sleep(2)  # Allow consumer to join group and rebalance

    # 2. Run Producer in main thread (generates 2 cycles for 4 tickers = 16 messages)
    target_tickers = ["AAPL", "TSLA", "NVDA", "MSFT"]
    run_ingestion_loop(tickers=target_tickers, iterations=2, interval_sec=2)

    # 3. Wait for consumer thread to drain the remaining messages
    consumer_thread.join()
    logger.info("=== End-to-End Pipeline Run Complete ===")

if __name__ == "__main__":
    run_end_to_end_pipeline()
