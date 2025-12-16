from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "teste",
    bootstrap_servers="kafka:9092",
    group_id="grupo-python",
    auto_offset_reset="earliest",
    api_version=(3, 6, 0),  
    value_deserializer=lambda v: v.decode("utf-8"),
)

print("Consumidor aguardando mensagens...")

for message in consumer:
    print(message.value, flush=True)
