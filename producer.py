from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='kafka:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

users = ['John', 'Peter', 'Sam']
locations = ['US', 'IN', 'FR', 'BR', 'JP']
devices = ['mobile', 'desktop', 'tablet']
merchants = ['Amazon', 'Walmart', 'Apple', 'Nike', 'eBay']
txn_types = ['purchase', 'transfer', 'withdrawal', 'deposit']

while True:
    data = {
        "user": random.choice(users),
        "amount": round(random.uniform(1.0, 500.0), 2),
        "location": random.choice(locations),
        "device": random.choice(devices),
        "merchant": random.choice(merchants),
        "txn_type": random.choice(txn_types),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send('transactions', data)
    time.sleep(0.5)