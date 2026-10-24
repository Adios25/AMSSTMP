import json
import time
import logging
from typing import List, Dict, Any
from kafka import KafkaConsumer
from src.cloud.lambda_function import lambda_handler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class CloudBridgeWorker:
    """
    Consumes raw feed messages from Kafka in micro-batches and invokes
    the serverless processing layer with manual offset commits.
    """
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "market-raw-feed",
        group_id: str = "amsstmp-cloud-bridge",
        batch_size: int = 5,
        max_poll_interval_ms: int = 300000
    ):
        self.topic = topic
        self.batch_size = batch_size
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Data Engineering Best Practice: Manual offset commit
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=batch_size,
            max_poll_interval_ms=max_poll_interval_ms
        )
        logger.info(f"Cloud Bridge initialized for topic '{topic}' [Consumer Group: {group_id}]")

    def process_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """
        Dispatches micro-batch to serverless handler.
        Returns True if processing succeeded.
        """
        try:
            event_payload = {"records": batch}
            response = lambda_handler(event_payload)
            return response.get("statusCode") == 200
        except Exception as e:
            logger.error(f"Error processing micro-batch in serverless handler: {e}")
            return False

    def start_bridge(self, max_batches: int = 0):
        """
        Main worker polling loop.
        max_batches=0 runs indefinitely until KeyboardInterrupt.
        """
        logger.info("Starting Bridge Consumer loop...")
        batch_count = 0

        try:
            while True:
                # Poll Kafka broker with 1000ms timeout
                msg_pack = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                
                batch_records: List[Dict[str, Any]] = []
                for tp, messages in msg_pack.items():
                    for message in messages:
                        batch_records.append(message.value)

                if batch_records:
                    logger.info(f"Collected micro-batch of {len(batch_records)} items. Dispatching...")
                    success = self.process_batch(batch_records)

                    if success:
                        # Commit offsets only upon verified persistence
                        self.consumer.commit()
                        batch_count += 1
                        logger.info(f"Batch {batch_count} processed & offsets committed successfully.")
                    else:
                        logger.warning("Batch processing failed. Offsets NOT committed; will retry on next poll.")

                    if max_batches > 0 and batch_count >= max_batches:
                        logger.info(f"Reached max requested batches ({max_batches}). Stopping bridge.")
                        break
                else:
                    time.sleep(0.5)

        except KeyboardInterrupt:
            logger.info("Bridge worker stopped by user.")
        finally:
            self.consumer.close()
            logger.info("Kafka Consumer closed.")

if __name__ == "__main__":
    bridge = CloudBridgeWorker(batch_size=4)
    bridge.start_bridge()
