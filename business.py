class OrderProcessor:
    def __init__(self, produtos, limite=10000):
        self.catalogo = {p["id"]: p for p in produtos}
        self.total = 0.0
        self.limite = limite
        self.alerta_disparado = False

    def process_order(self, evento):
        produto_id = evento["produto_id"]

        if produto_id not in self.catalogo:
            return "Produto não encontrado"

        produto = self.catalogo[produto_id]
        self.total += produto["preco"]

        msg = (
            f"Pedido: {produto['nome']} | "
            f"Preço: R$ {produto['preco']:.2f} | "
            f"Total acumulado: R$ {self.total:.2f}"
        )

        if self.total >= self.limite and not self.alerta_disparado:
            self.alerta_disparado = True
            msg += " >>> ALERTA: total de pedidos atingiu R$ 10.000 <<<"

        return msg
