import json
from kafka import KafkaConsumer

# === Configuração ===
LIMITE = 10000.0
TOTAL = 0.0

# === Carrega catálogo de produtos ===
with open("data.json", encoding="utf-8") as f:
    data = json.load(f)

# Mapeia produtos por ID
catalogo = {
    produto["id"]: produto
    for produto in data["produtos"]
}

consumer = KafkaConsumer(
    "pedidos",
    bootstrap_servers="kafka:9092",
    group_id="order-service",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("Consumer iniciado. Aguardando pedidos...\n")

alerta_disparado = False

for message in consumer:
    evento = message.value
    produto_id = evento.get("produto_id")

    if produto_id not in catalogo:
        print(f"Produto {produto_id} não encontrado\n")
        continue

    produto = catalogo[produto_id]
    preco = produto["preco"]
    nome = produto["nome"]

    TOTAL += preco

    print(
        f"Produto pedido: {nome} | "
        f"Preço: R$ {preco:.2f} | "
        f"Total acumulado: R$ {TOTAL:.2f}"
    )

    if TOTAL >= LIMITE and not alerta_disparado:
        alerta_disparado = True
        print("\n>>> ALERTA: TOTAL DE PEDIDOS ATINGIU OU ULTRAPASSOU R$ 10.000 <<<\n")
