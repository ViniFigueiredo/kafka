import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

with open("data.json", encoding="utf-8") as f:
    data = json.load(f)

carrinho = []

def mostrar_produtos():
    print("\n== LOJA ==")
    for p in data["produtos"]:
        print(f"ID: {p['id']} | {p['nome']} | R$ {p['preco']:.2f}")

def mostrar_carrinho():
    print("\n== CARRINHO ==")
    if not carrinho:
        print("Carrinho vazio")
        return
    for item in carrinho:
        print(f"Produto ID: {item['produto_id']} | Qtd: {item['quantidade']}")

def enviar_carrinho():
    producer.send("carrinho", {"itens": carrinho})
    producer.flush()

while True:
    try:
        mostrar_produtos()
        mostrar_carrinho()

        print("\n[A] Adicionar")
        print("[R] Remover")
        print("[F] Finalizar compra")
        print("[S] Sair")

        op = input("Opção: ").upper()

        if op == "A":
            pid = int(input("Produto ID: "))
            qtd = int(input("Quantidade: "))

            carrinho.append({"produto_id": pid, "quantidade": qtd})
            enviar_carrinho()

        elif op == "R":
            pid = int(input("Produto ID para remover: "))

            removido = next(
                (i for i in carrinho if i["produto_id"] == pid),
                None
            )

            if not removido:
                print("Produto não está no carrinho")
                continue

            producer.send(
                "carrinho",
                {
                    "status": "item_removido",
                    "item": removido
                }
            )
            producer.flush()

            carrinho[:] = [
                i for i in carrinho if i["produto_id"] != pid
            ]

            enviar_carrinho()

        elif op == "F":
            if not carrinho:
                print("Carrinho vazio")
                continue

            producer.send("compra", {"itens": carrinho})
            producer.flush()

            print("Compra finalizada com sucesso")
            break

        elif op == "S":
            if carrinho:
                producer.send(
                    "carrinho",
                    {
                        "status": "carrinho_abandonado",
                        "itens": carrinho
                    }
                )
                producer.flush()

            print("Encerrando aplicação")
            break

        else:
            print("Opção inválida")

    except ValueError:
        print("Digite apenas números")
