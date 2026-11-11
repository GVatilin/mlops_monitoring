import json
import logging
import os
import sys

import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka import Producer
from prometheus_client import start_http_server

sys.path.append(os.path.abspath('./src'))
from metrics import AMOUNT, ERRORS, FRAUD, FRAUD_RATIO, PROCESSING, SCORES, TRANSACTIONS
from preprocessing import run_preproc
from scorer import ensure_bootstrap, make_pred


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
        }
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)
        self.total = 0
        self.fraud = 0

    @PROCESSING.time()
    def process_message(self, msg):
        try:
            data = json.loads(msg.value().decode('utf-8'))

            transaction_id = data['transaction_id']
            input_df = pd.DataFrame([data['data']])

            processed_df = run_preproc(input_df)
            submission, _, model_version = make_pred(processed_df, "kafka_stream")

            submission['transaction_id'] = transaction_id
            submission['model_version'] = model_version
            for column in ('us_state', 'merch', 'cat_id', 'amount'):
                submission[column] = data['data'][column]

            delivery_errors = []
            self.producer.produce(
                SCORING_TOPIC,
                value=submission.to_json(orient='records'),
                on_delivery=lambda error, message: delivery_errors.append(error) if error else None,
            )
            self.producer.flush()
            if delivery_errors:
                raise RuntimeError(str(delivery_errors[0]))
            score = float(submission['score'].iloc[0])
            flag = int(submission['fraud_flag'].iloc[0])
            TRANSACTIONS.inc()
            SCORES.observe(score)
            FRAUD.inc(flag)
            AMOUNT.labels(fraud=str(flag)).inc(float(data['data']['amount']))
            self.total += 1
            self.fraud += flag
            FRAUD_RATIO.set(self.fraud / self.total)
            return True
        except Exception as e:
            ERRORS.inc()
            logger.exception(f"Error processing message: {e}")
            return False

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            
            self.process_message(msg)


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    ensure_bootstrap()
    start_http_server(8000)
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
