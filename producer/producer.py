from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092','localhost:9093','localhost:9094'],
    value_serializer = lambda x: json.dumps(x).encode('utf-8')
)

def generate_transaction():
    return {
        "transaction_id": random.randint(100000, 999999),
        "user_id": random.randint(1, 100),
        "amount": round(random.uniform(5, 2000), 2),
        "currency": random.choice(['USD', 'EUR', 'GBP']),
        "timestamp": datetime.now().isoformat(),
        "country": random.choice(["YE", "US", "AE", "SA", "FR", "IN", "CN"]),
        "merchant": random.choice(["Amazon", "Apple", "Google", "Shell", "Nike"])
    }

topic = "raw_transactions"

print("🚀 Starting data stream...")
while True:
    data = generate_transaction()

    producer.send(topic, value=data)
    print("Sent:", data)

    time.sleep(4)