import os
import json
import logging
from typing import Optional, Dict, Any
import boto3
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class AlertManager:
    """
    Evaluates sentiment anomalies and publishes alerts to Amazon SNS.
    """
    def __init__(self, spike_threshold: float = 0.60):
        self.spike_threshold = spike_threshold
        self.topic_arn = os.getenv("SNS_TOPIC_ARN")
        self.region = os.getenv("AWS_REGION", "us-east-1")
        
        # Initialize boto3 SNS client
        try:
            self.sns_client = boto3.client("sns", region_name=self.region)
        except Exception as e:
            logger.warning(f"Could not initialize AWS SNS client: {e}. Running in local mode.")
            self.sns_client = None

    def evaluate_and_notify(self, record: Dict[str, Any]) -> Optional[str]:
        """
        Checks if composite score exceeds threshold. If so, triggers SNS alert.
        Returns alert_type if triggered, otherwise None.
        """
        score = record["composite_score"]
        ticker = record["ticker"]
        alert_type = None

        if score >= self.spike_threshold:
            alert_type = "SPIKE_BULLISH"
        elif score <= -self.spike_threshold:
            alert_type = "SPIKE_BEARISH"

        if not alert_type:
            return None

        subject = f"🚨 Sentiment Alert: ${ticker} {alert_type} (Score: {score:+.2f})"
        message = (
            f"Autonomous Market Sentiment Alert\n"
            f"=================================\n"
            f"Ticker: ${ticker}\n"
            f"Alert Type: {alert_type}\n"
            f"Composite Score: {score:+.4f}\n"
            f"Primary Sentiment: {record['primary_sentiment']}\n"
            f"Source: {record['source']} ({record['content_type']})\n\n"
            f"Text Excerpt:\n\"{record['clean_text'][:280]}\"\n\n"
            f"Published At: {record['published_at']}\n"
        )

        logger.info(f"Triggering Alert: {subject}")

        # Attempt sending to AWS SNS if configured
        if self.sns_client and self.topic_arn and not self.topic_arn.startswith("arn:aws:sns:us-east-1:123456789012"):
            try:
                self.sns_client.publish(
                    TopicArn=self.topic_arn,
                    Subject=subject[:100],
                    Message=message
                )
                logger.info(f"SNS notification sent successfully for ${ticker}.")
            except Exception as e:
                logger.error(f"Failed to publish to Amazon SNS: {e}")
        else:
            logger.info(f"[LOCAL MOCK SNS] Alert broadcasted:\n{message}")

        return alert_type
