from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Fraud detector started...\n")

for message in consumer:
    data = message.value
    amount = data.get("amount", 0)

    if amount > 3000:
        print("FRAUD ALERT:", data)
    else:
        print("Normal transaction:", data)