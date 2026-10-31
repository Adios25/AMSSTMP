import json
import time
import logging
from typing import List, Dict, Any
from kafka import KafkaConsumer, TopicPartition
from src.cloud.lambda_function import lambda_handler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def process_published_records(metadata, bootstrap_servers="localhost:9092", timeout_seconds=30):
    """Process only the Kafka offsets produced by one dashboard run."""
    pending = {}
    for record in metadata:
        partition = TopicPartition(record.topic, record.partition)
        pending.setdefault(partition, set()).add(record.offset)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers, group_id=None,
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    processed = 0
    try:
        consumer.assign(list(pending))
        for partition, offsets in pending.items():
            consumer.seek(partition, min(offsets))
        last_activity = time.monotonic()
        while any(pending.values()):
            messages = consumer.poll(timeout_ms=1000, max_records=4)
            batch = [message for partition, records in messages.items()
                     for message in records if message.offset in pending[partition]]
            if batch:
                response = lambda_handler({"records": [message.value for message in batch]})
                if response.get("statusCode") != 200:
                    raise RuntimeError(f"Processing returned status {response.get('statusCode')}")
                for message in batch:
                    pending[TopicPartition(message.topic, message.partition)].remove(message.offset)
                processed += len(batch)
                last_activity = time.monotonic()
            elif time.monotonic() - last_activity >= timeout_seconds:
                raise TimeoutError(f"Timed out waiting for this run's Kafka records; processed {processed}.")
        return processed
    finally:
        consumer.close()

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

    def start_bridge(self, max_batches: int = 0, max_records: int = 0, max_idle_seconds: int = 0):
        """
        Main worker polling loop.
        max_batches=0 runs indefinitely until KeyboardInterrupt.
        """
        logger.info("Starting Bridge Consumer loop...")
        batch_count = 0
        record_count = 0
        last_activity = time.monotonic()

        try:
            while True:
                # Poll Kafka broker with 1000ms timeout
                msg_pack = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                
                batch_records: List[Dict[str, Any]] = []
                for tp, messages in msg_pack.items():
                    for message in messages:
                        batch_records.append(message.value)

                if batch_records:
                    last_activity = time.monotonic()
                    logger.info(f"Collected micro-batch of {len(batch_records)} items. Dispatching...")
                    success = self.process_batch(batch_records)

                    if success:
                        # Commit offsets only upon verified persistence
                        self.consumer.commit()
                        batch_count += 1
                        record_count += len(batch_records)
                        logger.info(f"Batch {batch_count} processed & offsets committed successfully.")
                    else:
                        logger.warning("Batch processing failed. Offsets NOT committed; will retry on next poll.")
                        if max_idle_seconds:
                            raise RuntimeError("Batch processing failed; check the terminal logs for the underlying error.")

                    if (max_batches > 0 and batch_count >= max_batches) or (max_records > 0 and record_count >= max_records):
                        logger.info(f"Reached max requested batches ({max_batches}). Stopping bridge.")
                        break
                else:
                    if max_idle_seconds and time.monotonic() - last_activity >= max_idle_seconds:
                        raise TimeoutError(f"No Kafka records received for {max_idle_seconds} seconds (processed {record_count} of {max_records} expected).")
                    time.sleep(0.5)

        except KeyboardInterrupt:
            logger.info("Bridge worker stopped by user.")
        finally:
            self.consumer.close()
            logger.info("Kafka Consumer closed.")
        return record_count

if __name__ == "__main__":
    bridge = CloudBridgeWorker(batch_size=4)
    bridge.start_bridge()
