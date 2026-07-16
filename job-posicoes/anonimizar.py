"""
Máscara determinística de dados sensíveis (conta, nome, valor monetário).

Usada quando ANONYMIZE=true — hoje só no cliente interno de testes, que roda
com dados reais do BTG da Creta mas não pode expor conta/valor real.

Determinístico e sem estado: o mesmo valor real + o mesmo ANON_SALT sempre
produzem o mesmo valor mascarado — sem precisar de tabela de mapeamento
persistida. Sem o salt (guardado só no Secret Manager do projeto que usa a
máscara), não dá pra reverter nem fazer força bruta.
"""

import hashlib


def _hash_int(valor: str, salt: str, mod: int) -> int:
    h = hashlib.sha256(f"{salt}:{valor}".encode()).hexdigest()
    return int(h[:8], 16) % mod


def _normalizar_conta(conta) -> str:
    """
    Conta aparece formatada de jeitos diferentes entre tabelas (STRING com
    espaço/zero à esquerda em posicao_das_contas, INTEGER limpo em
    conta_assessor_base/receitas_para_repasse — mesmo padrão do
    SAFE_CAST(TRIM(...)) já usado nos JOINs do sistema). Precisa normalizar
    antes de gerar o hash, senão a mesma conta vira pseudônimos diferentes
    em tabelas diferentes.
    """
    s = str(conta).strip()
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


def mascarar_conta(conta_real, salt: str) -> str:
    """
    Pseudônimo puramente numérico (string de dígitos) — Conta é STRING em
    algumas tabelas (posicao_das_contas) e INTEGER em outras
    (conta_assessor_base, receitas_para_repasse), com JOIN entre elas via
    SAFE_CAST. Um pseudônimo com prefixo textual quebraria isso.
    """
    if conta_real is None or str(conta_real).strip() == "":
        return conta_real
    n = _hash_int(_normalizar_conta(conta_real), salt, 9_000_000) + 1_000_000  # 7 dígitos
    return str(n)


def mascarar_nome(nome_real, salt: str) -> str:
    if nome_real is None or str(nome_real).strip() == "":
        return nome_real
    n = _hash_int(str(nome_real).strip(), salt, 10_000)
    return f"Assessor {n:04d}"


def fator_valor(conta_real, salt: str) -> float:
    """Fator fixo por conta (0.6–1.4x) — mantém a forma dos dados sem expor o valor real."""
    n = _hash_int(_normalizar_conta(conta_real), salt, 1000)
    return 0.6 + (n / 1000) * 0.8


def mascarar_valor(valor_real, conta_real, salt: str):
    if valor_real is None:
        return valor_real
    try:
        return round(float(valor_real) * fator_valor(conta_real, salt), 2)
    except (TypeError, ValueError):
        return valor_real
