from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
import joblib
from datetime import datetime

model = joblib.load("fraud_model.pkl")

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fraud-detector'
)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Fraud detection consumer running...")

user_last_location = {}

for message in consumer:
    txn = message.value

    # RULE-BASED DETECTIONS
    suspicious = False
    reasons = []

    # 1. High amount
    if txn["amount"] > 400:
        suspicious = True
        reasons.append("High amount")

    # 2. Location mismatch in short time
    user = txn["user"]
    if user in user_last_location and user_last_location[user] != txn["location"]:
        suspicious = True
        reasons.append("Location change from {} to {}".format(user_last_location[user], txn["location"]))
    user_last_location[user] = txn["location"]

    # 3. Suspicious merchant
    if txn["merchant"] in ["XYZ_Merchant", "UnknownShop"]:
        suspicious = True
        reasons.append(f"Merchant {txn['merchant']} flagged")

    # ML ANOMALY DETECTION
    data_point = pd.DataFrame([{
        "amount": txn["amount"],
        "txn_per_hour": 3,   # later we can track actual frequency
        # Additional numeric fields can be added for ML
    }])

    ml_result = model.predict(data_point)[0]
    if ml_result == -1:
        suspicious = True
        reasons.append("ML anomaly detection")

    txn["fraudulent"] = suspicious
    txn["reason"] = reasons

    if suspicious:
        print(f"Fraud Alert: {txn}")
        producer.send('fraud_alerts', txn)
    else:
        print(f"Legit Txn: {txn}")
        producer.send('classified_transactions', txn)
