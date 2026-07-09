"""
job-carteira-recomendada — Cloud Run Job
Busca carteira recomendada de equities (allocation + portfolio) e salva no BigQuery.
Roda toda segunda-feira às 8h (America/Sao_Paulo) via Cloud Scheduler.
"""

import base64
import logging
import os
import uuid
from datetime import date

import pandas as pd
import requests
from google.cloud import bigquery

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("job-carteira-recomendada")

# ── Configuração ──────────────────────────────────────────────────────────────
CLIENT_ID     = os.environ["BTG_CLIENT_ID"]
CLIENT_SECRET = os.environ["BTG_CLIENT_SECRET"]
GCP_PROJECT   = os.getenv("GCP_PROJECT", "creta-btg")
DATASET       = os.getenv("BQ_DATASET",  "dados_crus")

URL_AUTH      = "https://api.btgpactual.com/iaas-auth/api/v1/authorization/oauth2/accesstoken"
BASE_EQUITIES = "https://api.btgpactual.com/iaas-recommended-equities"

# ── Autenticação ──────────────────────────────────────────────────────────────

def gerar_token() -> str:
    credencial = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        URL_AUTH,
        headers={
            "Authorization": f"Basic {credencial}",
            "Content-Type": "application/x-www-form-urlencoded",
            "x-id-partner-request": str(uuid.uuid4()),
        },
        data={"grant_type": "client_credentials"},
    )
    r.raise_for_status()
    token = r.headers.get("access_token")
    if not token:
        raise Exception(f"Token não encontrado. Headers: {dict(r.headers)}")
    log.info("Token gerado com sucesso")
    return token

# ── Requisição ────────────────────────────────────────────────────────────────

def req_equities(token, method, path, body=None):
    r = requests.request(
        method, f"{BASE_EQUITIES}{path}",
        headers={
            "access_token": token,
            "x-id-partner-request": str(uuid.uuid4()),
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        json=body,
    )
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text

# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_allocation(dados: list) -> pd.DataFrame:
    linhas = []
    for carteira in dados:
        nome     = carteira.get("name")
        tipo     = carteira.get("typeInitial")
        inicio   = carteira.get("validityStart", "")[:10]
        fim      = carteira.get("validityEnd", "")[:10]
        rent_ant = carteira.get("previousProfitability")
        rent_acu = carteira.get("accumulatedProfitability")
        idx_ant  = carteira.get("previousIndex")
        idx_acu  = carteira.get("accumulatedIndex")

        for item in carteira.get("assets", []):
            asset = item.get("asset", {})
            linhas.append({
                "Carteira":         nome,
                "Tipo":             tipo,
                "Inicio":           inicio,
                "Fim":              fim,
                "Rentab_Anterior":  rent_ant,
                "Rentab_Acumulada": rent_acu,
                "Indice_Anterior":  idx_ant,
                "Indice_Acumulado": idx_acu,
                "Ticker":           asset.get("ticker"),
                "Empresa":          asset.get("company"),
                "Setor":            asset.get("sector", {}).get("name"),
                "Peso":             item.get("weight"),
            })
    df = pd.DataFrame(linhas)
    log.info(f"allocation: {len(df)} ativos em {df['Carteira'].nunique()} carteira(s)")
    return df


def parse_portfolio(dados: list) -> pd.DataFrame:
    linhas = []
    for carteira in dados:
        nome  = carteira.get("name") or carteira.get("id")
        idx   = carteira.get("accumulatedIndex")
        rent  = carteira.get("accumulatedProfitability")
        bench = carteira.get("comparativeIndex")

        for item in carteira.get("inAssets", []):
            asset = item.get("asset", {})
            fund  = asset.get("currentFundamentals", {})
            linhas.append({
                "Carteira":         nome,
                "Benchmark":        bench,
                "Indice_Acumulado": idx,
                "Rentab_Acumulada": rent,
                "Empresa":          asset.get("company"),
                "Setor":            asset.get("sector", {}).get("name"),
                "EV_EBITDA":        fund.get("evEbitda"),
                "PL":               fund.get("pl"),
                "PVP":              fund.get("pvp"),
            })
    df = pd.DataFrame(linhas)
    log.info(f"portfolio: {len(df)} ativos em {df['Carteira'].nunique()} carteira(s)")
    return df

# ── BigQuery ──────────────────────────────────────────────────────────────────

def salvar_bigquery(df_allocation: pd.DataFrame, df_portfolio: pd.DataFrame):
    client = bigquery.Client(project=GCP_PROJECT)
    hoje   = date.today().strftime("%Y%m%d")

    for df, tabela in [
        (df_allocation, "carteira_recomendada_allocation"),
        (df_portfolio,  "carteira_recomendada_portfolio"),
    ]:
        if df.empty:
            log.warning(f"{tabela} vazio — pulando")
            continue

        df_bq = df.copy()
        df_bq["DataExtracao"] = pd.to_datetime(date.today())

        table_id = f"{GCP_PROJECT}.{DATASET}.{tabela}${hoje}"
        job = client.load_table_from_dataframe(
            df_bq,
            destination=table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="DataExtracao",
                ),
            ),
        )
        job.result()
        log.info(f"{tabela} — {len(df_bq)} linha(s) salvas → partição {hoje}")

# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== Job Carteira Recomendada iniciado ===")

    token = gerar_token()

    status_a, dados_allocation = req_equities(token, "GET", "/api/v1/recommended-equities-allocation")
    status_p, dados_portfolio  = req_equities(token, "GET", "/api/v1/recommended-equities-allocation/portfolio")

    if status_a != 200:
        log.error(f"Erro ao buscar allocation: HTTP {status_a} — {dados_allocation}")
        raise SystemExit(1)
    if status_p != 200:
        log.error(f"Erro ao buscar portfolio: HTTP {status_p} — {dados_portfolio}")
        raise SystemExit(1)

    df_allocation = parse_allocation(dados_allocation)
    df_portfolio  = parse_portfolio(dados_portfolio)

    salvar_bigquery(df_allocation, df_portfolio)

    log.info("=== Job concluído ===")
