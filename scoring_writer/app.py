import json
import logging
import os

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
WRITTEN = Counter("writer_scores_total", "Scores committed to PostgreSQL")
FRAUD = Counter("writer_fraud_total", "Committed scores flagged as fraud")
WRITE_TIME = Histogram("writer_write_seconds", "PostgreSQL insert and commit time")
SCORES = Histogram("writer_score", "Committed fraud probabilities", buckets=[i / 20 for i in range(21)])


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scores (
                id BIGSERIAL PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                fraud_flag INT NOT NULL,
                model_version TEXT,
                created_at TIMESTAMPTZ DEFAULT clock_timestamp()
            );
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS us_state TEXT;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS merch TEXT;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS cat_id TEXT;
            ALTER TABLE scores ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION;
            CREATE INDEX IF NOT EXISTS scores_created_at_idx ON scores (created_at);
        """)
    conn.commit()


@WRITE_TIME.time()
def insert_score(conn, data):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO scores (transaction_id, score, fraud_flag, model_version, us_state, merch, cat_id, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(data.get(key) for key in (
            "transaction_id", "score", "fraud_flag", "model_version", "us_state", "merch", "cat_id", "amount"
        )))
    conn.commit()
    WRITTEN.inc()
    FRAUD.inc(data["fraud_flag"])
    SCORES.observe(data["score"])


def run_consumer():
    conn = psycopg2.connect(**{
        key: os.environ[f"POSTGRES_{env}"] for key, env in (
            ("host", "HOST"), ("port", "PORT"), ("dbname", "DB"), ("user", "USER"), ("password", "PASSWORD")
        )
    })
    create_table(conn)
    start_http_server(8001)
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": "scoring-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([os.environ["KAFKA_SCORING_TOPIC"]])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            try:
                for data in json.loads(msg.value()):
                    insert_score(conn, data)
                consumer.commit(message=msg, asynchronous=False)
            except Exception:
                conn.rollback()
                logger.exception("Не удалось записать скор")
                raise
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run_consumer()
