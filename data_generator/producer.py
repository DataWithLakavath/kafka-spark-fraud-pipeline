from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "user_id": random.randint(1,100),
        "amount": random.randint(10,5000),
        "location": random.choice(["US","IND","UK"]),
        "time": time.time()
    }

    producer.send("transactions", data)
    print("Transaction sent:", data)

    time.sleep(2)