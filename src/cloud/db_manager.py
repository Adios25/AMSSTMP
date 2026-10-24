import os
import logging
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages PostgreSQL relational persistence with connection handling
    and bulk idempotent upserts.
    """
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = int(port or os.getenv("DB_PORT", "5432"))
        self.dbname = dbname or os.getenv("DB_NAME", "postgres")
        self.user = user or os.getenv("DB_USER", "postgres")
        self.password = password or os.getenv("DB_PASSWORD", "postgres")

    def get_connection(self):
        """Creates a direct connection to PostgreSQL."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            connect_timeout=5
        )

    def init_schema(self, schema_sql_path: str = "src/database/schema.sql") -> None:
        """Executes DDL script to initialize all tables and indexes."""
        with open(schema_sql_path, "r") as f:
            ddl = f.read()

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        logger.info("Database schema initialized successfully.")

    def save_sentiment_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        Idempotently inserts a batch of processed sentiment metrics.
        Returns the count of rows successfully persisted.
        """
        if not records:
            return 0

        insert_query = """
            INSERT INTO fact_sentiment_metrics (
                id, ticker, source, content_type, clean_text, url,
                vader_compound, finbert_positive, finbert_negative, finbert_neutral,
                composite_score, primary_sentiment, published_at
            ) VALUES %s
            ON CONFLICT (id) DO NOTHING;
        """

        values = [
            (
                r["id"],
                r["ticker"],
                r["source"],
                r["content_type"],
                r["clean_text"],
                r.get("url"),
                r.get("vader_compound"),
                r.get("finbert_positive"),
                r.get("finbert_negative"),
                r.get("finbert_neutral"),
                r["composite_score"],
                r["primary_sentiment"],
                r["published_at"]
            )
            for r in records
        ]

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, values)
                inserted_count = cur.rowcount
            conn.commit()

        logger.info(f"Persisted {inserted_count} / {len(records)} records to fact_sentiment_metrics.")
        return inserted_count

    def log_alert(self, message_id: str, ticker: str, alert_type: str, score: float, target: str) -> None:
        """Logs an event-driven alert record into fact_sentiment_alerts."""
        query = """
            INSERT INTO fact_sentiment_alerts (message_id, ticker, alert_type, score, notification_target)
            VALUES (%s, %s, %s, %s, %s);
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (message_id, ticker, alert_type, score, target))
            conn.commit()
        logger.info(f"Logged alert [{alert_type}] for ${ticker} (Score: {score}).")
