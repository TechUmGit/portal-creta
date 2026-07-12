"""
job-prioridades — Cloud Run Job
Roda diariamente, calcula prioridades por assessor via BigQuery
e salva os resultados no Firestore (coleção: prioridades/{assessor_slug}).
"""

import logging
import os
import unicodedata
from datetime import datetime, timezone

from google.cloud import bigquery, firestore

from regras import regra_auc_queda, regra_caixa, regra_sem_receita, regra_vencimentos

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("job-prioridades")

# ── Config ────────────────────────────────────────────────────────────────────
GCP_PROJECT = os.getenv("GCP_PROJECT", "creta-btg")
DATASET     = os.getenv("BQ_DATASET",  "dados_crus")
FS_PROJECT  = os.getenv("FS_PROJECT",  "creta-btg-bd3a8")

TABLE          = f"`{GCP_PROJECT}.{DATASET}.receitas_para_repasse`"
TABLE_POSICAO  = f"`{GCP_PROJECT}.{DATASET}.posicao_das_contas`"
TABLE_EXCECOES      = f"`{GCP_PROJECT}.{DATASET}.conta_assessor_excecoes`"
TABLE_ASSESSOR_BASE = f"`{GCP_PROJECT}.{DATASET}.conta_assessor_base`"

ORDEM_PRIORIDADE = {"alta": 0, "media": 1, "baixa": 2}


# ── Helpers ───────────────────────────────────────────────────────────────────

def slug(nome: str) -> str:
    """'Manu Lombardi' → 'manu_lombardi'"""
    sem_acento = unicodedata.normalize("NFD", nome)
    sem_acento = "".join(c for c in sem_acento if unicodedata.category(c) != "Mn")
    return sem_acento.lower().strip().replace(" ", "_")


def mapa_assessor_sq() -> str:
    """Subquery que devolve (Conta INT64, Assessor STRING) com prioridade para exceções."""
    return f"""(
        SELECT Conta, Assessor
        FROM (
            SELECT e.Conta, e.Assessor, 1 AS prioridade
            FROM {TABLE_EXCECOES} e
            WHERE e.DataInicio <= CURRENT_DATE()
              AND (e.DataFim IS NULL OR e.DataFim >= CURRENT_DATE())
            UNION ALL
            SELECT b.Conta, b.Assessor, 2 AS prioridade
            FROM {TABLE_ASSESSOR_BASE} b
            WHERE b.MesRef = (SELECT MAX(MesRef) FROM {TABLE_ASSESSOR_BASE})
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY Conta ORDER BY prioridade) = 1
    )"""


def ser_row(row) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(row).items()}


def rq(bq: bigquery.Client, sql: str) -> list[dict]:
    return [ser_row(r) for r in bq.query(sql).result()]


def agrupar_por_assessor(linhas: list[dict]) -> dict[str, list[dict]]:
    grupos: dict[str, list[dict]] = {}
    for r in linhas:
        a = (r.get("assessor") or "").strip().upper()
        if not a:
            continue
        grupos.setdefault(a, []).append(r)
    return grupos


# ── Queries BigQuery (sem filtro de assessor — retorna tudo) ──────────────────

def query_caixa(bq: bigquery.Client) -> list[dict]:
    sql = f"""
        WITH ultima_data AS (SELECT MAX(DATE(Data)) AS d FROM {TABLE_POSICAO}),
        posicao_total AS (
            SELECT TRIM(p.Conta) AS Conta, ROUND(SUM(p.ValorBruto),2) AS total_auc
            FROM {TABLE_POSICAO} p CROSS JOIN ultima_data ud
            WHERE DATE(p.Data) = ud.d AND p.Classe != 'Aluguel de Ações'
            GROUP BY TRIM(p.Conta)
        ),
        caixa AS (
            SELECT TRIM(p.Conta) AS Conta, ROUND(SUM(p.ValorBruto),2) AS valor_caixa
            FROM {TABLE_POSICAO} p CROSS JOIN ultima_data ud
            WHERE DATE(p.Data) = ud.d AND p.Classe = 'Caixa'
            GROUP BY TRIM(p.Conta)
        ),
        nomes AS (
            SELECT Conta AS conta_num, MAX(Cliente) AS cliente
            FROM {TABLE} WHERE Cliente IS NOT NULL AND Cliente != '' GROUP BY Conta
        )
        SELECT
            c.Conta,
            COALESCE(n.cliente, '') AS cliente,
            UPPER(COALESCE(ma.Assessor,'')) AS assessor,
            c.valor_caixa,
            t.total_auc
        FROM caixa c
        LEFT JOIN posicao_total t ON c.Conta = t.Conta
        LEFT JOIN nomes n ON SAFE_CAST(c.Conta AS INT64) = n.conta_num
        LEFT JOIN {mapa_assessor_sq()} ma ON SAFE_CAST(c.Conta AS INT64) = ma.Conta
        WHERE c.valor_caixa >= 5000
        ORDER BY c.valor_caixa DESC
    """
    return rq(bq, sql)


def query_vencimentos(bq: bigquery.Client) -> list[dict]:
    sql = f"""
        WITH ultima_data AS (SELECT MAX(DATE(Data)) AS d FROM {TABLE_POSICAO}),
        nomes AS (
            SELECT Conta AS conta_num, MAX(Cliente) AS cliente
            FROM {TABLE} WHERE Cliente IS NOT NULL AND Cliente != '' GROUP BY Conta
        )
        SELECT
            TRIM(p.Conta) AS Conta,
            COALESCE(n.cliente,'') AS cliente,
            UPPER(COALESCE(ma.Assessor,'')) AS assessor,
            p.Classe, p.Nome,
            p.Vencimento,
            DATE_DIFF(DATE(p.Vencimento), CURRENT_DATE(), DAY) AS dias_para_vencer,
            ROUND(SUM(p.ValorBruto),2) AS valor
        FROM {TABLE_POSICAO} p
        CROSS JOIN ultima_data ud
        LEFT JOIN nomes n ON SAFE_CAST(TRIM(p.Conta) AS INT64) = n.conta_num
        LEFT JOIN {mapa_assessor_sq()} ma ON SAFE_CAST(TRIM(p.Conta) AS INT64) = ma.Conta
        WHERE DATE(p.Data) = ud.d
          AND p.Vencimento IS NOT NULL AND p.Vencimento != ''
          AND DATE(p.Vencimento) >= CURRENT_DATE()
          AND DATE(p.Vencimento) <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
          AND p.Classe NOT IN ('Ação','Caixa','Aluguel de Ações','Derivativo')
        GROUP BY TRIM(p.Conta), cliente, assessor, p.Classe, p.Nome, p.Vencimento
        ORDER BY p.Vencimento
    """
    return rq(bq, sql)


def query_sem_receita(bq: bigquery.Client) -> list[dict]:
    sql = f"""
        WITH ultima_data AS (SELECT MAX(DATE(Data)) AS d FROM {TABLE_POSICAO}),
        contas_ativas AS (
            SELECT TRIM(p.Conta) AS Conta, ROUND(SUM(p.ValorBruto),2) AS auc_atual
            FROM {TABLE_POSICAO} p CROSS JOIN ultima_data ud
            WHERE DATE(p.Data) = ud.d AND p.Classe != 'Aluguel de Ações'
            GROUP BY TRIM(p.Conta)
        ),
        ultima_receita AS (
            SELECT CAST(Conta AS STRING) AS Conta, MAX(DATE(Data_De_Referencia)) AS ultima_data
            FROM {TABLE}
            GROUP BY Conta
        ),
        nomes AS (
            SELECT Conta AS conta_num, MAX(Cliente) AS cliente
            FROM {TABLE} WHERE Cliente IS NOT NULL AND Cliente != '' GROUP BY Conta
        )
        SELECT
            ca.Conta,
            COALESCE(n.cliente,'') AS cliente,
            UPPER(COALESCE(ma.Assessor,'')) AS assessor,
            ca.auc_atual,
            ur.ultima_data AS ultima_receita,
            DATE_DIFF(CURRENT_DATE(), COALESCE(ur.ultima_data, DATE('2020-01-01')), DAY) AS dias_sem_receita
        FROM contas_ativas ca
        LEFT JOIN ultima_receita ur ON ca.Conta = ur.Conta
        LEFT JOIN nomes n ON SAFE_CAST(ca.Conta AS INT64) = n.conta_num
        LEFT JOIN {mapa_assessor_sq()} ma ON SAFE_CAST(ca.Conta AS INT64) = ma.Conta
        WHERE (ur.ultima_data IS NULL OR ur.ultima_data < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
          AND ca.auc_atual > 1000
        ORDER BY dias_sem_receita DESC
    """
    return rq(bq, sql)


def query_auc_queda(bq: bigquery.Client) -> list[dict]:
    sql = f"""
        WITH ultima_data AS (SELECT MAX(DATE(Data)) AS d FROM {TABLE_POSICAO}),
        data_60 AS (
            SELECT MAX(DATE(p.Data)) AS d
            FROM {TABLE_POSICAO} p CROSS JOIN ultima_data ud
            WHERE DATE(p.Data) <= DATE_SUB(ud.d, INTERVAL 60 DAY)
        ),
        auc_atual AS (
            SELECT TRIM(p.Conta) AS Conta, ROUND(SUM(p.ValorBruto),2) AS auc
            FROM {TABLE_POSICAO} p CROSS JOIN ultima_data ud
            WHERE DATE(p.Data) = ud.d AND p.Classe != 'Aluguel de Ações'
            GROUP BY TRIM(p.Conta)
        ),
        auc_ant AS (
            SELECT TRIM(p.Conta) AS Conta, ROUND(SUM(p.ValorBruto),2) AS auc
            FROM {TABLE_POSICAO} p CROSS JOIN data_60 d60
            WHERE DATE(p.Data) = d60.d AND p.Classe != 'Aluguel de Ações'
            GROUP BY TRIM(p.Conta)
        ),
        nomes AS (
            SELECT Conta AS conta_num, MAX(Cliente) AS cliente
            FROM {TABLE} WHERE Cliente IS NOT NULL AND Cliente != '' GROUP BY Conta
        )
        SELECT
            a.Conta,
            COALESCE(n.cliente,'') AS cliente,
            UPPER(COALESCE(ma.Assessor,'')) AS assessor,
            a.auc AS auc_atual,
            r.auc AS auc_60d,
            ROUND(a.auc - r.auc, 2) AS delta,
            CASE WHEN r.auc > 0 THEN ROUND((a.auc - r.auc) / r.auc * 100, 1) ELSE NULL END AS pct_var
        FROM auc_atual a
        INNER JOIN auc_ant r ON a.Conta = r.Conta
        LEFT JOIN nomes n ON SAFE_CAST(a.Conta AS INT64) = n.conta_num
        LEFT JOIN {mapa_assessor_sq()} ma ON SAFE_CAST(a.Conta AS INT64) = ma.Conta
        WHERE a.auc < r.auc
        ORDER BY delta ASC
    """
    return rq(bq, sql)


# ── Processamento por assessor ────────────────────────────────────────────────

def processar_assessor(
    assessor: str,
    caixa:       list[dict],
    vencimentos: list[dict],
    sem_receita: list[dict],
    auc_queda:   list[dict],
) -> list[dict]:
    itens: list[dict] = []
    itens += regra_caixa(caixa)
    itens += regra_vencimentos(vencimentos)
    itens += regra_sem_receita(sem_receita)
    itens += regra_auc_queda(auc_queda)

    # Ordena: alta → media → baixa
    itens.sort(key=lambda x: ORDEM_PRIORIDADE.get(x["prioridade"], 9))
    return itens


# ── Firestore ─────────────────────────────────────────────────────────────────

def salvar_firestore(fs: firestore.Client, assessor: str, itens: list[dict]):
    doc_id = slug(assessor)
    fs.collection("prioridades").document(doc_id).set({
        "assessor":   assessor,
        "gerado_em":  datetime.now(timezone.utc).isoformat(),
        "itens":      itens,
    })
    log.info(f"  Firestore → prioridades/{doc_id} ({len(itens)} item(s))")


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== Job Prioridades iniciado ===")

    bq_client = bigquery.Client(project=GCP_PROJECT)
    fs_client = firestore.Client(project=FS_PROJECT)

    log.info("Consultando BigQuery...")
    dados_caixa      = query_caixa(bq_client)
    dados_venc       = query_vencimentos(bq_client)
    dados_sem_rec    = query_sem_receita(bq_client)
    dados_auc_queda  = query_auc_queda(bq_client)
    log.info(
        f"Dados carregados — caixa:{len(dados_caixa)} "
        f"venc:{len(dados_venc)} sem_rec:{len(dados_sem_rec)} queda:{len(dados_auc_queda)}"
    )

    # Agrupa cada dataset por assessor
    g_caixa    = agrupar_por_assessor(dados_caixa)
    g_venc     = agrupar_por_assessor(dados_venc)
    g_sem_rec  = agrupar_por_assessor(dados_sem_rec)
    g_queda    = agrupar_por_assessor(dados_auc_queda)

    # União de todos os assessores que aparecem em qualquer dataset
    todos_assessores = (
        set(g_caixa) | set(g_venc) | set(g_sem_rec) | set(g_queda)
    )
    log.info(f"{len(todos_assessores)} assessor(es) encontrado(s)")

    for assessor in sorted(todos_assessores):
        log.info(f"Processando: {assessor}")
        itens = processar_assessor(
            assessor,
            g_caixa.get(assessor, []),
            g_venc.get(assessor, []),
            g_sem_rec.get(assessor, []),
            g_queda.get(assessor, []),
        )
        if itens:
            salvar_firestore(fs_client, assessor, itens)
        else:
            log.info(f"  Sem prioridades para {assessor} — documento não atualizado")

    log.info("=== Job Prioridades concluído ===")
