"""
job-posicoes — Cloud Run Job
Busca posições de todas as contas BTG e salva no BigQuery.
Roda diariamente às 8h (America/Sao_Paulo) via Cloud Scheduler.
"""

import base64
import io
import json
import logging
import os
import time
import uuid
import zipfile
from datetime import date, timedelta

import pandas as pd
import requests
from google.cloud import bigquery

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("job-posicoes")

# ── Configuração (via variáveis de ambiente) ──────────────────────────────────
CLIENT_ID     = os.environ["BTG_CLIENT_ID"]
CLIENT_SECRET = os.environ["BTG_CLIENT_SECRET"]
GCP_PROJECT   = os.getenv("GCP_PROJECT",  "creta-btg")
DATASET       = os.getenv("BQ_DATASET",   "dados_crus")
DATA_INICIO   = os.getenv("DATA_INICIO",  "2025-05-01")

URL_AUTH     = "https://api.btgpactual.com/iaas-auth/api/v1/authorization/oauth2/accesstoken"
BASE         = "https://api.btgpactual.com/iaas-api-position"
BASE_ADVISOR = "https://api.btgpactual.com/iaas-account-advisor"

TABELA_POSICAO = f"{GCP_PROJECT}.{DATASET}.posicao_das_contas"

CONFIG = {"project_id": GCP_PROJECT, "dataset_id": DATASET}

COLUNAS = [
    "Conta", "Data", "Classe", "Subclasse",
    "Nome", "Ticker", "ISIN", "Emissor",
    "Indexador", "Vencimento", "Direcao",
    "Quantidade", "Preco", "ValorBruto", "IR", "IOF", "ValorLiquido",
    "InfoExtra",
]

CHAVES_RAIZ_COBERTAS = {
    "AccountNumber", "PositionDate",
    "FixedIncome", "InvestmentFund", "Equities", "Cash",
    "FixedIncomeStructuredNote", "CryptoCoin", "PensionInformations",
    "PendingSettlements", "CashCollateral",
    "Derivative", "Credits", "PayableReceivables",
    "ContractVersion", "Agency", "TotalAmmount", "SummaryAccounts", "EventCreateDate",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _linha(**kwargs) -> dict:
    return {col: kwargs.get(col) for col in COLUNAS}

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

# ── Requisições ───────────────────────────────────────────────────────────────

def req(token, method, path, params=None, body=None):
    r = requests.request(
        method, f"{BASE}{path}",
        headers={
            "access_token": token,
            "x-id-partner-request": str(uuid.uuid4()),
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        params=params, json=body,
    )
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text

def req_advisor(token, path, params=None):
    r = requests.get(
        f"{BASE_ADVISOR}{path}",
        headers={
            "access_token": token,
            "x-id-partner-request": str(uuid.uuid4()),
            "Accept": "application/json",
        },
        params=params,
    )
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text

# ── Contas ────────────────────────────────────────────────────────────────────

def obter_contas(token) -> list:
    status, dados = req_advisor(token, "/api/v1/advisor/accounts")
    if status != 200:
        log.warning(f"HTTP {status} ao buscar contas: {dados}")
        return []
    contas = dados.get("accounts", [])
    log.info(f"{len(contas)} conta(s) obtida(s)")
    return contas

# ── Parser de posição ─────────────────────────────────────────────────────────

def parse_posicao(dados: dict) -> pd.DataFrame:
    conta = dados.get("AccountNumber")
    data  = dados.get("PositionDate", "")[:10]
    linhas = []

    for item in dados.get("FixedIncome", []):
        linhas.append(_linha(
            Conta=conta, Data=data, Classe="Renda Fixa",
            Subclasse=item.get("IssuerType"), Nome=item.get("AccountingGroupCode"),
            Ticker=item.get("Ticker"), ISIN=item.get("ISIN"), Emissor=item.get("Issuer"),
            Indexador=item.get("IndexYieldRate"), Vencimento=item.get("MaturityDate", "")[:10],
            Quantidade=float(item.get("Quantity", 0)), Preco=float(item.get("Price", 0)),
            ValorBruto=float(item.get("GrossValue", 0)), IR=float(item.get("IncomeTax", 0)),
            IOF=float(item.get("IOFTax", 0)), ValorLiquido=float(item.get("NetValue", 0)),
        ))

    for item in dados.get("InvestmentFund", []):
        fundo = item.get("Fund", {})
        for aq in item.get("Acquisition", [{}]):
            linhas.append(_linha(
                Conta=conta, Data=data, Classe="Fundo",
                Subclasse=fundo.get("BenchMark"), Nome=fundo.get("FundName"),
                Emissor=fundo.get("ManagerName"),
                Quantidade=float(aq.get("NumberOfShares", 0)), Preco=float(item.get("ShareValue", 0)),
                ValorBruto=float(aq.get("GrossAssetValue", 0)), IR=float(aq.get("IncomeTax", 0)),
                IOF=float(aq.get("VirtualIOF", 0)), ValorLiquido=float(aq.get("NetAssetValue", 0)),
                InfoExtra=f"CNPJ: {fundo.get('FundCNPJCode')}",
            ))

    for eq in dados.get("Equities", []):
        for op in eq.get("OptionPositions", []):
            avg = op.get("AveragePrice", {})
            ativo_ref = op.get("ReferenceAsset", {}).get("Ticker", "")
            linhas.append(_linha(
                Conta=conta, Data=data, Classe="Derivativo",
                Subclasse=f"Opção {op.get('OptionType')}", Nome=op.get("Ticker"), Ticker=op.get("Ticker"),
                Direcao=op.get("BuySell"), Vencimento=op.get("MaturityDate", "")[:10],
                Quantidade=float(op.get("Quantity", 0)), Preco=float(op.get("MarketPremium", 0)),
                ValorBruto=float(op.get("TotalValue", 0)), ValorLiquido=float(op.get("TotalValue", 0)),
                InfoExtra=f"Strike: {op.get('StrikePrice')} | Ref: {ativo_ref} | PM: {avg.get('Price')}",
            ))
        for ac in eq.get("StockPositions", []):
            linhas.append(_linha(
                Conta=conta, Data=data, Classe="Ação",
                Subclasse=ac.get("SectorDescription"), Nome=ac.get("Description"),
                Ticker=ac.get("Ticker"), ISIN=ac.get("ISINCode"),
                Quantidade=float(ac.get("Quantity", 0)), Preco=float(ac.get("MarketPrice", 0)),
                ValorBruto=float(ac.get("GrossValue", 0)), IR=float(ac.get("IncomeTax", 0)),
                ValorLiquido=float(ac.get("GrossValue", 0)) - float(ac.get("IncomeTax", 0)),
                InfoExtra=f"PrevClose: {ac.get('PrevClose')} | Tipo: {ac.get('EquityTypeDescription')}",
            ))
        for al in eq.get("StockLendingPositions", []):
            linhas.append(_linha(
                Conta=conta, Data=data, Classe="Aluguel de Ações",
                Subclasse=al.get("LendingType"), Nome=al.get("Ticker"), Ticker=al.get("Ticker"),
                Vencimento=al.get("MaturityDate", "")[:10],
                Quantidade=float(al.get("Quantity", 0)), Preco=float(al.get("MarketPrice", 0)),
                ValorBruto=float(al.get("MarketValue", 0)), IR=float(al.get("IRTax", 0)),
                ValorLiquido=float(al.get("TotalValue", 0)),
                InfoExtra=f"Taxa: {al.get('RatePorcent')}% | Início: {al.get('TransactionDate','')[:10]}",
            ))

    for cash in dados.get("Cash", []):
        cc = cash.get("CurrentAccount", {})
        if cc.get("Value"):
            linhas.append(_linha(
                Conta=conta, Data=data, Classe="Caixa", Subclasse="Conta Corrente", Nome="Conta Corrente",
                ValorBruto=float(cc.get("Value", 0)), ValorLiquido=float(cc.get("Value", 0)),
            ))
        for ci in cash.get("CashInvested", []):
            nome = ci.get("Name", {})
            linhas.append(_linha(
                Conta=conta, Data=data, Classe="Caixa", Subclasse="Caixa Investido",
                Nome=nome.get("Nome") if isinstance(nome, dict) else nome,
                Indexador=nome.get("Indexador") if isinstance(nome, dict) else None,
                Vencimento=ci.get("MaturityDate", "")[:10],
                Quantidade=float(ci.get("Quantity", 0)), Preco=float(ci.get("CostPrice", 0)),
                ValorBruto=float(ci.get("GrossValue", 0)), IR=float(ci.get("IncomeTax", 0)),
                IOF=float(ci.get("IofTax", 0)), ValorLiquido=float(ci.get("NetValue", 0)),
            ))

    for item in dados.get("FixedIncomeStructuredNote", []):
        linhas.append(_linha(
            Conta=conta, Data=data, Classe="COE",
            Subclasse=item.get("AccountingGroupCode"),
            Nome=item.get("FantasyName") or item.get("AccountingGroupCode"),
            Ticker=item.get("Ticker"), Emissor=item.get("Issuer"),
            Indexador=item.get("ReferenceIndexName"), Vencimento=item.get("MaturityDate", "")[:10],
            Quantidade=float(item.get("Quantity", 0)), Preco=float(item.get("Price", 0)),
            ValorBruto=float(item.get("GrossValue", 0)), IR=float(item.get("IncomeTax", 0)),
            IOF=float(item.get("IOFTax", 0)), ValorLiquido=float(item.get("NetValue", 0)),
            InfoExtra=f"Índice: {item.get('ReferenceIndexValue')} | PM: {item.get('CostPrice')}",
        ))

    for item in dados.get("CryptoCoin", []):
        asset = item.get("Asset", {})
        linhas.append(_linha(
            Conta=conta, Data=data, Classe="Cripto",
            Subclasse=asset.get("Type"), Nome=asset.get("Name"), Ticker=asset.get("Code"),
            Quantidade=float(item.get("Quantity", 0)), Preco=float(item.get("MarketPrice", 0)),
            ValorBruto=float(item.get("GrossFinancial", 0)), IR=float(item.get("IncomeTax", 0)),
            IOF=float(item.get("IOFTax", 0)), ValorLiquido=float(item.get("Financial", 0)),
            InfoExtra=f"CostBasis: {item.get('CostBasis')} | Código: {asset.get('ProductCode')}",
        ))

    for item in dados.get("PensionInformations", []):
        linhas.append(_linha(
            Conta=conta, Data=data, Classe="Previdência",
            Subclasse=item.get("FundType"), Nome=item.get("CertificateName"),
            Emissor=item.get("CorporateName"), Indexador=item.get("TaxRegime"),
            Preco=float(item.get("CostPrice", 0)),
            ValorBruto=float(item.get("GrossValue", 0)), ValorLiquido=float(item.get("NetValue", 0)),
            InfoExtra=f"CNPJ: {item.get('CorporateCNPJ')} | Status: {item.get('CertificateStatus')} | Renda: {item.get('IncomeType')}",
        ))

    for ps in dados.get("PendingSettlements", []):
        for sub in ["Equities", "FixedIncome"]:
            for item in ps.get(sub, []):
                linhas.append(_linha(
                    Conta=conta, Data=data, Classe="Provento a Receber",
                    Subclasse=item.get("Transaction"), Nome=item.get("Description"),
                    Ticker=item.get("Ticker"), Vencimento=item.get("SettlementDate", "")[:10],
                    ValorBruto=float(item.get("FinancialValue", 0)),
                    ValorLiquido=float(item.get("FinancialValue", 0)),
                ))

    for item in dados.get("CashCollateral", []):
        linhas.append(_linha(
            Conta=conta, Data=data, Classe="Garantia",
            Subclasse=item.get("ReserveType"), Nome=item.get("BlockedMethod"),
            ValorBruto=float(item.get("FinancialValue", 0)),
            ValorLiquido=float(item.get("FinancialValue", 0)),
            InfoExtra=f"Protocolo: {item.get('Protocol')}",
        ))

    return pd.DataFrame(linhas, columns=COLUNAS)

# ── BigQuery ──────────────────────────────────────────────────────────────────

def datas_ja_salvas() -> set:
    client = bigquery.Client(project=GCP_PROJECT)
    try:
        df = client.query(
            f"SELECT DISTINCT CAST(Data AS STRING) AS Data FROM `{TABELA_POSICAO}`"
        ).to_dataframe()
        salvas = set(df["Data"].str[:10].tolist())
        log.info(f"{len(salvas)} datas já na tabela — serão puladas")
        return salvas
    except Exception:
        log.info("Tabela ainda não existe — processando tudo")
        return set()

def salvar_particao(df: pd.DataFrame, data_ref: str):
    if df.empty:
        log.warning(f"DataFrame vazio — pulando {data_ref}")
        return
    df = df.copy()
    df["Data"] = pd.to_datetime(data_ref)
    client = bigquery.Client(project=GCP_PROJECT)
    particao       = data_ref.replace("-", "")
    tabela_particao = f"{TABELA_POSICAO}${particao}"
    job = client.load_table_from_dataframe(df, tabela_particao, job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="Data"
        ),
        autodetect=True,
    ))
    job.result()
    log.info(f"{len(df)} posições → partição {data_ref}")

# ── Busca de posição ──────────────────────────────────────────────────────────

def buscar_posicao_historica(token, conta, data_ref) -> dict:
    status, dados = req(token, "POST", f"/api/v1/position/{conta}", body={"date": data_ref})
    if status == 429:
        log.warning(f"Rate limit ({conta} / {data_ref}), aguardando 10s...")
        time.sleep(10)
        status, dados = req(token, "POST", f"/api/v1/position/{conta}", body={"date": data_ref})
    if status != 200 or "AccountNumber" not in dados:
        return {}
    return dados

# ── Job principal ─────────────────────────────────────────────────────────────

def consolidar_historico(token, lista_contas: list):
    inicio = date.fromisoformat(DATA_INICIO)
    fim    = date.today()

    datas = []
    d = inicio
    while d <= fim:
        if d.weekday() < 5:          # pula fins de semana
            datas.append(str(d))
        d += timedelta(days=1)

    ja_salvas       = datas_ja_salvas()
    datas_pendentes = [d for d in datas if d not in ja_salvas]
    log.info(f"{len(datas_pendentes)} datas a processar ({len(datas) - len(datas_pendentes)} já salvas)")
    log.info(f"{len(lista_contas)} contas × {len(datas_pendentes)} datas = {len(datas_pendentes) * len(lista_contas):,} requisições")

    token_gerado_em = time.time()
    erros_total     = []

    for i_data, data_ref in enumerate(datas_pendentes, 1):
        log.info(f"[{i_data}/{len(datas_pendentes)}] {data_ref}")
        partes, erros_data = [], []

        for i_conta, conta in enumerate(lista_contas, 1):
            # Renova token a cada 50 min
            if time.time() - token_gerado_em > 50 * 60:
                log.info("Renovando token...")
                token = gerar_token()
                token_gerado_em = time.time()

            if i_conta % 100 == 0 or i_conta == len(lista_contas):
                log.info(f"  [{i_conta}/{len(lista_contas)}]")

            dados = buscar_posicao_historica(token, conta, data_ref)
            if dados:
                partes.append(parse_posicao(dados))
            else:
                erros_data.append((data_ref, conta))

        if partes:
            salvar_particao(pd.concat(partes, ignore_index=True), data_ref)
        else:
            log.warning(f"Nenhuma posição para {data_ref}")

        erros_total.extend(erros_data)

    log.info("Histórico concluído.")
    if erros_total:
        log.warning(f"{len(erros_total)} falhas:")
        for e in erros_total[:20]:
            log.warning(f"  {e}")

    return erros_total


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== Job Posições iniciado ===")

    token  = gerar_token()
    contas = obter_contas(token)

    if not contas:
        log.error("Nenhuma conta obtida. Encerrando.")
        raise SystemExit(1)

    erros = consolidar_historico(token, contas)

    if erros:
        log.warning(f"Job concluído com {len(erros)} erros.")
    else:
        log.info("=== Job concluído sem erros ===")
