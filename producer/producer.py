import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Digite o ID do produto para realizar um pedido")

while True:
    try:
        produto_id = int(input("Produto ID: "))

        evento = {
            "produto_id": produto_id
        }

        producer.send("pedidos", evento)
        producer.flush()

        print(f"Pedido enviado: {evento}")

    except ValueError:
        print("Digite apenas números inteiros")
