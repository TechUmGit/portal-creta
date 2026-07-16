#!/usr/bin/env python3
"""
Copia tabelas do BigQuery da Creta (dados já carregados) pro projeto de um
cliente de teste, aplicando a mesma máscara determinística usada pelo
job-posicoes (job-posicoes/anonimizar.py) — conta e nome viram pseudônimo
estável, valores monetários são escalados por um fator fixo por conta.

Uso:
  ANON_SALT="valor-do-secret" ./scripts/copiar-e-mascarar-historico.py interno-portal

O salt deve ser o mesmo configurado no secret `anon-salt` do projeto destino
(assim os pseudônimos batem com os que o job-posicoes vai gerar dali pra
frente pros dados novos).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "job-posicoes"))
from anonimizar import mascarar_conta, mascarar_nome, mascarar_valor  # noqa: E402

from google.cloud import bigquery

REF_PROJECT = "creta-btg"
REF_DATASET = "dados_crus"
DEST_DATASET = "dados_crus"

# Por tabela: quais colunas são conta (pseudônimo numérico), nome (pseudônimo
# de pessoa, com rótulo pra não confundir Cliente com Assessor) e valor
# monetário (escalado pelo fator da conta da própria linha).
TABELAS_CONFIG = {
    "posicao_das_contas": {
        "colunas_conta": ["Conta"],
        "colunas_nome": {},
        "colunas_valor": ["ValorBruto", "ValorLiquido", "Preco"],
    },
    "conta_assessor_base": {
        "colunas_conta": ["Conta"],
        "colunas_nome": {"Assessor": "Assessor"},
        "colunas_valor": [],
    },
    "conta_assessor_excecoes": {
        "colunas_conta": ["Conta"],
        "colunas_nome": {"Assessor": "Assessor", "CriadoPor": "Assessor"},
        "colunas_valor": [],
    },
    "receitas_para_repasse": {
        "colunas_conta": ["Conta"],
        "colunas_nome": {
            "Cliente": "Cliente",
            "Codigo_Assessor": "Assessor",
            "Assessor_Principal": "Assessor",
            "Assessor_Manual": "Assessor",
        },
        "colunas_valor": ["Receita_Bruta", "Receita_Liquida", "Comissao",
                           "Comissao_Liquida", "Repasse_Total_liquido"],
    },
    "suitability_contas": {
        "colunas_conta": ["Conta"],
        "colunas_nome": {},
        "colunas_valor": [],
    },
}


def mascarar_tabela(df, config, salt):
    conta_col = config["colunas_conta"][0] if config["colunas_conta"] else None

    for col in config["colunas_valor"]:
        if conta_col:
            df[col] = df.apply(
                lambda r: mascarar_valor(r[col], r[conta_col], salt), axis=1
            )

    for col, rotulo in config["colunas_nome"].items():
        df[col] = df[col].apply(lambda v: mascarar_nome(v, salt, rotulo))

    for col in config["colunas_conta"]:
        tipo_original = df[col].dtype
        df[col] = df[col].apply(lambda v: mascarar_conta(v, salt))
        # Preserva o tipo original da coluna (INTEGER em algumas tabelas,
        # STRING em outras) — mascarar_conta sempre devolve dígitos, então
        # cabe nos dois casos.
        if tipo_original.kind in "iu":
            df[col] = df[col].astype("Int64")

    return df


def main():
    if len(sys.argv) not in (2, 3):
        print("Uso: copiar-e-mascarar-historico.py <projeto-destino> [tabela]", file=sys.stderr)
        sys.exit(1)
    dest_project = sys.argv[1]
    tabela_filtro = sys.argv[2] if len(sys.argv) == 3 else None

    salt = os.environ.get("ANON_SALT")
    if not salt:
        print("Defina ANON_SALT (mesmo valor do secret 'anon-salt' do projeto destino).", file=sys.stderr)
        sys.exit(1)

    client_origem = bigquery.Client(project=REF_PROJECT)
    client_destino = bigquery.Client(project=dest_project)

    tabelas = {tabela_filtro: TABELAS_CONFIG[tabela_filtro]} if tabela_filtro else TABELAS_CONFIG

    for tabela, config in tabelas.items():
        origem = f"{REF_PROJECT}.{REF_DATASET}.{tabela}"
        destino = f"{dest_project}.{DEST_DATASET}.{tabela}"

        print(f"── {tabela} ──")
        try:
            df = client_origem.query(f"SELECT * FROM `{origem}`").to_dataframe()
        except Exception as e:
            print(f"  ⚠ não consegui ler {origem}: {e}")
            continue

        if df.empty:
            print("  (vazia, pulei)")
            continue

        df = mascarar_tabela(df, config, salt)

        job = client_destino.load_table_from_dataframe(
            df, destino,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        job.result()
        print(f"  ✓ {len(df)} linha(s) copiada(s) e mascaradas → {destino}")

    print("\nConcluído.")


if __name__ == "__main__":
    main()
