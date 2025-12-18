import json
from kafka import KafkaConsumer

with open("data.json", encoding="utf-8") as f:
    data = json.load(f)

catalogo = {p["id"]: p for p in data["produtos"]}

consumer = KafkaConsumer(
    "carrinho",
    "compra",
    bootstrap_servers="kafka:9092",
    group_id="order-service-v3",
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("Consumer monitorando atividade de compra...\n")

for msg in consumer:
    evento = msg.value

    # ===== ITEM REMOVIDO =====
    if msg.topic == "carrinho" and evento.get("status") == "item_removido":
        item = evento["item"]
        produto = catalogo.get(item["produto_id"])

        if produto:
            print(
                f"\n ITEM REMOVIDO: {produto['nome']} | "
                f"Qtd: {item['quantidade']}"
            )
        continue

    # ===== CARRINHO =====
    if msg.topic == "carrinho":
        itens = evento.get("itens", [])

        if evento.get("status") == "carrinho_abandonado":
            print("\nCARRINHO ABANDONADO")
        else:
            print("\n=== ESTADO DO CARRINHO ===")

        if not itens:
            print("Carrinho vazio")
            continue

        total = 0.0

        for item in itens:
            produto = catalogo.get(item["produto_id"])
            if not produto:
                continue

            valor = produto["preco"] * item["quantidade"]
            total += valor

            print(
                f"{produto['nome']} | "
                f"Qtd: {item['quantidade']} | "
                f"R$ {valor:.2f}"
            )

        print(f"TOTAL DO CARRINHO: R$ {total:.2f}")

    # ===== COMPRA =====
    elif msg.topic == "compra":
        print("\n=== COMPRA FINALIZADA ===")

        total = 0.0

        for item in evento.get("itens", []):
            produto = catalogo.get(item["produto_id"])
            if not produto:
                continue

            valor = produto["preco"] * item["quantidade"]
            total += valor

            print(
                f"{produto['nome']} | "
                f"Qtd: {item['quantidade']} | "
                f"R$ {valor:.2f}"
            )

        print(f"TOTAL DA COMPRA: R$ {total:.2f}")
