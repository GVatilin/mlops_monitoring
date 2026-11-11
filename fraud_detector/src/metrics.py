from prometheus_client import Counter, Gauge, Histogram

TRANSACTIONS = Counter("scoring_transactions_total", "Successfully published scores")
PROCESSING = Histogram("scoring_processing_seconds", "Processing and Kafka delivery time")
SCORES = Histogram("scoring_score", "Predicted fraud probability", buckets=[i / 20 for i in range(21)])
FRAUD = Counter("scoring_fraud_total", "Transactions flagged as fraud")
FRAUD_RATIO = Gauge("scoring_fraud_ratio", "Fraud fraction since service start")
AMOUNT = Counter("scoring_amount_total", "Processed payment amount", ["fraud"])
ERRORS = Counter("scoring_errors_total", "Failed transaction processing attempts")
MODEL_LOADS = Counter("scoring_model_loads_total", "Successful model loads", ["version"])
MODEL_RELOADED = Gauge("scoring_model_reloaded_timestamp_seconds", "Last hot reload time", ["version"])
