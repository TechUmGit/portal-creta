"""
regras.py — Funções de regra para geração de prioridades por assessor.

Cada função recebe os dados brutos do BigQuery (já filtrados para 1 assessor)
e retorna uma lista de itens de prioridade, ou lista vazia se não houver alerta.

Estrutura de cada item:
{
    "tipo":       str,   # caixa | vencimento | sem_receita | auc_queda
    "prioridade": str,   # alta | media | baixa
    "titulo":     str,
    "descricao":  str,
    "conta_destaque": str | None
}
"""

from __future__ import annotations

from datetime import date

# Vencimentos abaixo desse valor não contam como prioridade (ruído).
VALOR_MINIMO_VENCIMENTO = 3_000


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dias_ate_fim_da_semana() -> int:
    """Quantos dias faltam até domingo da semana atual (0 se hoje já é domingo)."""
    hoje = date.today()
    return (6 - hoje.weekday()) % 7  # weekday(): segunda=0 ... domingo=6

def _fmt(valor: float) -> str:
    """Formata valor em R$ com separadores brasileiros (sem centavos)."""
    if valor is None:
        return "—"
    if valor >= 1_000_000:
        return f"R$ {valor / 1_000_000:,.1f}M".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {valor:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _nivel(valor: float, limiar_alto: float, limiar_medio: float) -> str:
    if valor >= limiar_alto:
        return "alta"
    if valor >= limiar_medio:
        return "media"
    return "baixa"


# ── Regra 1: Caixa Parado ─────────────────────────────────────────────────────

def regra_caixa(dados: list[dict]) -> list[dict]:
    """
    dados: linhas com Conta, cliente, assessor, valor_caixa, total_auc

    Threshold:
        alta  → total caixa > R$ 200k
        media → total caixa entre R$ 50k e R$ 200k
        baixa → total caixa entre R$ 5k e R$ 50k
    """
    if not dados:
        return []

    total = sum(r["valor_caixa"] or 0 for r in dados)
    destaque = max(dados, key=lambda r: r["valor_caixa"] or 0)
    n = len(dados)

    prioridade = _nivel(total, 200_000, 50_000)

    if n == 1:
        descricao = (
            f"{n} cliente com {_fmt(total)} em caixa parado sem rendimento. "
            f"Conta: {destaque['cliente'] or destaque['Conta']} ({_fmt(destaque['valor_caixa'])})."
        )
    else:
        descricao = (
            f"{n} clientes com {_fmt(total)} em caixa parado sem rendimento. "
            f"Maior posição: {destaque['cliente'] or destaque['Conta']} ({_fmt(destaque['valor_caixa'])})."
        )

    return [{
        "tipo": "caixa",
        "prioridade": prioridade,
        "titulo": "Caixa parado",
        "descricao": descricao,
        "conta_destaque": str(destaque["Conta"]),
    }]


# ── Regra 2: Vencimentos ──────────────────────────────────────────────────────

def regra_vencimentos(dados: list[dict]) -> list[dict]:
    """
    dados: linhas com Conta, cliente, assessor, dias_para_vencer, valor

    Gera até 2 itens: um para vencimentos "esta semana" (até domingo da
    semana atual — não uma janela de 7 dias corridos) e um para 8-30 dias.
    Vencimentos abaixo de VALOR_MINIMO_VENCIMENTO são ignorados (ruído).
    """
    if not dados:
        return []

    dados = [r for r in dados if (r.get("valor") or 0) >= VALOR_MINIMO_VENCIMENTO]
    if not dados:
        return []

    fim_semana = _dias_ate_fim_da_semana()
    itens_7  = [r for r in dados if (r["dias_para_vencer"] or 999) <= fim_semana]
    itens_30 = [r for r in dados if fim_semana < (r["dias_para_vencer"] or 999) <= 30]

    resultado = []

    if itens_7:
        total = sum(r["valor"] or 0 for r in itens_7)
        mais_proximo = min(itens_7, key=lambda r: r["dias_para_vencer"] or 999)
        n = len(itens_7)
        descricao = (
            f"{n} título(s) vence(m) esta semana, totalizando {_fmt(total)}. "
            f"Mais próximo: {mais_proximo['cliente'] or mais_proximo['Conta']} "
            f"em {mais_proximo['dias_para_vencer']}d."
        )
        resultado.append({
            "tipo": "vencimento",
            "prioridade": "alta",
            "titulo": "Vencimentos esta semana",
            "descricao": descricao,
            "conta_destaque": str(mais_proximo["Conta"]),
        })

    if itens_30:
        total = sum(r["valor"] or 0 for r in itens_30)
        n = len(itens_30)
        mais_proximo = min(itens_30, key=lambda r: r["dias_para_vencer"] or 999)
        descricao = (
            f"{n} título(s) vence(m) nos próximos 30 dias, totalizando {_fmt(total)}. "
            f"Próximo: {mais_proximo['cliente'] or mais_proximo['Conta']} "
            f"em {mais_proximo['dias_para_vencer']}d."
        )
        resultado.append({
            "tipo": "vencimento",
            "prioridade": "media",
            "titulo": "Vencimentos próximos (30 dias)",
            "descricao": descricao,
            "conta_destaque": str(mais_proximo["Conta"]),
        })

    return resultado


# ── Regra 3: Sem Receita ──────────────────────────────────────────────────────

def regra_sem_receita(dados: list[dict]) -> list[dict]:
    """
    dados: linhas com Conta, cliente, assessor, auc_atual, dias_sem_receita

    Threshold:
        alta  → 3+ clientes com 60+ dias sem receita
        media → 1-2 clientes com 60+ dias, ou 3+ clientes com 30-59 dias
        baixa → 1-2 clientes com 30-59 dias
    """
    if not dados:
        return []

    sem_60 = [r for r in dados if (r["dias_sem_receita"] or 0) >= 60]
    sem_30 = [r for r in dados if 30 <= (r["dias_sem_receita"] or 0) < 60]

    resultado = []

    if sem_60:
        mais_antigo = max(sem_60, key=lambda r: r["dias_sem_receita"] or 0)
        n = len(sem_60)
        prioridade = "alta" if n >= 3 else "media"
        descricao = (
            f"{n} cliente(s) sem receita há mais de 60 dias. "
            f"Mais antigo: {mais_antigo['cliente'] or mais_antigo['Conta']} "
            f"({mais_antigo['dias_sem_receita']}d sem movimentação)."
        )
        resultado.append({
            "tipo": "sem_receita",
            "prioridade": prioridade,
            "titulo": "Clientes sem receita (60+ dias)",
            "descricao": descricao,
            "conta_destaque": str(mais_antigo["Conta"]),
        })

    if sem_30:
        mais_antigo = max(sem_30, key=lambda r: r["dias_sem_receita"] or 0)
        n = len(sem_30)
        prioridade = "media" if n >= 3 else "baixa"
        descricao = (
            f"{n} cliente(s) sem receita entre 30 e 60 dias. "
            f"Atenção: {mais_antigo['cliente'] or mais_antigo['Conta']} ({mais_antigo['dias_sem_receita']}d)."
        )
        resultado.append({
            "tipo": "sem_receita",
            "prioridade": prioridade,
            "titulo": "Clientes sem receita (30-60 dias)",
            "descricao": descricao,
            "conta_destaque": str(mais_antigo["Conta"]),
        })

    return resultado


# ── Regra 4: Queda de AuC ────────────────────────────────────────────────────

def regra_auc_queda(dados: list[dict]) -> list[dict]:
    """
    dados: linhas com Conta, cliente, assessor, auc_atual, auc_60d, delta, pct_var

    Threshold (delta é negativo = queda):
        alta  → 2+ clientes com queda > R$ 100k  OU  1 cliente com queda > R$ 500k
        media → 1 cliente com queda > R$ 100k   OU  2+ clientes com queda > R$ 30k
        baixa → 1 cliente com queda entre R$ 30k e R$ 100k
    """
    if not dados:
        return []

    # delta é negativo; abs(delta) = tamanho da queda
    quedas_grandes = [r for r in dados if abs(r["delta"] or 0) > 100_000]
    quedas_medias  = [r for r in dados if 30_000 < abs(r["delta"] or 0) <= 100_000]

    resultado = []

    if quedas_grandes:
        maior = max(quedas_grandes, key=lambda r: abs(r["delta"] or 0))
        n = len(quedas_grandes)
        total_queda = sum(abs(r["delta"] or 0) for r in quedas_grandes)

        if n >= 2 or abs(maior["delta"] or 0) > 500_000:
            prioridade = "alta"
        else:
            prioridade = "media"

        descricao = (
            f"{n} cliente(s) com queda de AuC acima de R$ 100k nos últimos 60 dias "
            f"(total: -{_fmt(total_queda)}). "
            f"Maior queda: {maior['cliente'] or maior['Conta']} "
            f"(-{_fmt(abs(maior['delta']))} / {maior['pct_var']}%)."
        )
        resultado.append({
            "tipo": "auc_queda",
            "prioridade": prioridade,
            "titulo": "Queda de AuC",
            "descricao": descricao,
            "conta_destaque": str(maior["Conta"]),
        })

    elif quedas_medias:
        maior = max(quedas_medias, key=lambda r: abs(r["delta"] or 0))
        n = len(quedas_medias)
        prioridade = "media" if n >= 2 else "baixa"
        descricao = (
            f"{n} cliente(s) com queda de AuC entre R$ 30k e R$ 100k. "
            f"Maior: {maior['cliente'] or maior['Conta']} (-{_fmt(abs(maior['delta']))})."
        )
        resultado.append({
            "tipo": "auc_queda",
            "prioridade": prioridade,
            "titulo": "Queda de AuC",
            "descricao": descricao,
            "conta_destaque": str(maior["Conta"]),
        })

    return resultado
