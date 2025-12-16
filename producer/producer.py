from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: v.encode("utf-8"),
)

print("Digite mensagens para enviar ao Kafka:")

while True:
    msg = input()
    producer.send("teste", msg)
    producer.flush()
    print(f"Enviado: {msg}")
