import json
from kafka import KafkaConsumer

# === CONFIGURAÇÃO ===
LIMITE = 10000.0
alerta_disparado = False

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

    if msg.topic == "carrinho" and evento.get("status") == "item_removido":
        item = evento["item"]
        produto = catalogo.get(item["produto_id"])

        if produto:
            print(
                f"\nITEM REMOVIDO: {produto['nome']} | "
                f"Qtd: {item['quantidade']}"
            )
        continue

    if msg.topic == "carrinho":
        itens = evento.get("itens", [])
        TOTAL = 0.0
        alerta_disparado = False  # reseta a cada novo estado do carrinho

        if evento.get("status") == "carrinho_abandonado":
            print("\nCARRINHO ABANDONADO")
        else:
            print("\n=== ESTADO DO CARRINHO ===")

        if not itens:
            print("Carrinho vazio")
            continue

        for item in itens:
            produto = catalogo.get(item["produto_id"])
            if not produto:
                continue

            valor = produto["preco"] * item["quantidade"]
            TOTAL += valor

            print(
                f"{produto['nome']} | "
                f"Qtd: {item['quantidade']} | "
                f"R$ {valor:.2f}"
            )

        print(f"TOTAL DO CARRINHO: R$ {TOTAL:.2f}")

        if TOTAL >= LIMITE and not alerta_disparado:
            alerta_disparado = True
            print(
                "\n>>> ALERTA: TOTAL DO CARRINHO ULTRAPASSOU R$ 10.000 <<<\n"
            )

    # ================= COMPRA =================
    elif msg.topic == "compra":
        print("\n=== COMPRA FINALIZADA ===")
        TOTAL = 0.0
        alerta_disparado = False

        for item in evento.get("itens", []):
            produto = catalogo.get(item["produto_id"])
            if not produto:
                continue

            valor = produto["preco"] * item["quantidade"]
            TOTAL += valor

            print(
                f"{produto['nome']} | "
                f"Qtd: {item['quantidade']} | "
                f"R$ {valor:.2f}"
            )

        print(f"TOTAL DA COMPRA: R$ {TOTAL:.2f}")

        if TOTAL >= LIMITE and not alerta_disparado:
            alerta_disparado = True
            print(
                "\n>>> ALERTA: VALOR DA COMPRA ULTRAPASSOU R$ 10.000 <<<\n"
            )
