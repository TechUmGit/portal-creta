"""
job-suitability — Cloud Run Job
Busca perfil de suitability de todas as contas BTG e salva no BigQuery.
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
log = logging.getLogger("job-suitability")

# ── Configuração ──────────────────────────────────────────────────────────────
CLIENT_ID     = os.environ["BTG_CLIENT_ID"]
CLIENT_SECRET = os.environ["BTG_CLIENT_SECRET"]
GCP_PROJECT   = os.getenv("GCP_PROJECT", "creta-btg")
DATASET       = os.getenv("BQ_DATASET",  "dados_crus")

URL_AUTH         = "https://api.btgpactual.com/iaas-auth/api/v1/authorization/oauth2/accesstoken"
BASE_ADVISOR     = "https://api.btgpactual.com/iaas-account-advisor"
BASE_SUITABILITY = "https://api.btgpactual.com/iaas-suitability"

TABELA = f"{GCP_PROJECT}.{DATASET}.suitability_contas"

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

# ── Contas ────────────────────────────────────────────────────────────────────

def obter_contas(token) -> list:
    r = requests.get(
        f"{BASE_ADVISOR}/api/v1/advisor/accounts",
        headers={
            "access_token": token,
            "x-id-partner-request": str(uuid.uuid4()),
            "Accept": "application/json",
        },
    )
    dados = r.json()
    contas = dados.get("accounts", [])
    log.info(f"{len(contas)} conta(s) obtida(s)")
    return contas

# ── Suitability ───────────────────────────────────────────────────────────────

def req_suitability(token, path):
    r = requests.get(
        f"{BASE_SUITABILITY}{path}",
        headers={
            "access_token": token,
            "x-id-partner-request": str(uuid.uuid4()),
            "Accept": "application/json",
        },
    )
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text


def consolidar_suitability(token, lista_contas) -> pd.DataFrame:
    linhas, erros = [], []
    total = len(lista_contas)

    for i, conta in enumerate(lista_contas, 1):
        if i % 50 == 0 or i == total:
            log.info(f"  [{i}/{total}]")

        status, dados = req_suitability(token, f"/api/v1/suitability/account/{conta}/info")

        if status == 200:
            linhas.append({
                "Conta":         conta,
                "Perfil":        dados.get("description"),
                "Codigo":        dados.get("code"),
                "DataInicio":    (dados.get("initDate")        or "")[:10],
                "DataExpiracao": (dados.get("expirationDate")  or "")[:10],
            })
        else:
            erros.append(conta)

    if erros:
        log.warning(f"{len(erros)} conta(s) com erro")

    df = pd.DataFrame(linhas)
    log.info(f"{len(df)} perfis carregados")
    return df, erros


def salvar_bigquery(df: pd.DataFrame):
    client = bigquery.Client(project=GCP_PROJECT)

    hoje            = date.today().strftime("%Y%m%d")
    tabela_particao = f"{TABELA}${hoje}"

    df_bq = df.copy()
    df_bq["DataExtracao"] = pd.to_datetime(date.today())
    df_bq["DataInicio"]    = pd.to_datetime(df_bq["DataInicio"],    errors="coerce")
    df_bq["DataExpiracao"] = pd.to_datetime(df_bq["DataExpiracao"], errors="coerce")

    job = client.load_table_from_dataframe(
        df_bq,
        destination=tabela_particao,
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
    log.info(f"{len(df_bq)} perfis salvos → partição {hoje}")


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== Job Suitability iniciado ===")

    token  = gerar_token()
    contas = obter_contas(token)

    if not contas:
        log.error("Nenhuma conta obtida. Encerrando.")
        raise SystemExit(1)

    df, erros = consolidar_suitability(token, contas)

    if df.empty:
        log.error("Nenhum perfil obtido. Encerrando.")
        raise SystemExit(1)

    salvar_bigquery(df)

    if erros:
        log.warning(f"Job concluído com {len(erros)} erros.")
    else:
        log.info("=== Job concluído sem erros ===")
