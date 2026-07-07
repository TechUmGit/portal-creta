"""
api-creta — Cloud Run service
Serve dados do BigQuery para o Portal Creta.

Endpoints:
  GET /api/receitas?periodo=12m   → todos os dados da página Receitas e Repasses
  GET /health                     → health check
"""

import io
import os
import re
import time
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import quote
import pandas as pd
import yfinance as yf

from fastapi import FastAPI, Depends, HTTPException, Header, Query, UploadFile, File, status
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import bigquery, storage as gcs
from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig
import firebase_admin
from firebase_admin import credentials, auth as firebase_auth, firestore as fb_firestore

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api-creta")

# ── Firebase Admin (verifica tokens JWT do frontend) ─────────────────────────
# Em Cloud Run: a variável GOOGLE_APPLICATION_CREDENTIALS aponta para o arquivo
# de service account. Em dev local, use: firebase_admin.initialize_app() que
# detecta o ADC (Application Default Credentials).
if not firebase_admin._apps:
    firebase_admin.initialize_app(options={"projectId": "creta-btg-bd3a8"})

# ── BigQuery client ───────────────────────────────────────────────────────────
# Project ID e dataset — configuráveis via variável de ambiente
GCP_PROJECT    = os.getenv("GCP_PROJECT", "creta-btg")
bq = bigquery.Client(project=GCP_PROJECT)
DATASET        = os.getenv("BQ_DATASET",  "dados_crus")
TABLE          = f"`{GCP_PROJECT}.{DATASET}.receitas_para_repasse`"
TABLE_POSICAO     = f"`{GCP_PROJECT}.{DATASET}.posicao_das_contas`"
TABLE_SUITABILITY = f"`{GCP_PROJECT}.{DATASET}.suitability_contas`"
TABLE_EXCECOES      = f"`{GCP_PROJECT}.{DATASET}.conta_assessor_excecoes`"
TABLE_ASSESSOR_BASE = f"`{GCP_PROJECT}.{DATASET}.conta_assessor_base`"

# ── GCS ───────────────────────────────────────────────────────────────────────
GCS_BUCKET  = os.getenv("GCS_BUCKET", "creta-btg-pipeline")
GCS_PREFIX  = "entradas/"
gcs_client  = gcs.Client(project=GCP_PROJECT)

# ── Cotações via Yahoo Finance ────────────────────────────────────────────────
# Tickers BTG internos → código B3 padrão (.SA é o sufixo do Yahoo para B3)
TICKER_MAP_YF: dict[str, str] = {
    "VALEON": "VALE3.SA",
    "PETRPN": "PETR4.SA",
    "PETRPP": "PETR3.SA",
    "BRADPN": "BBDC4.SA",
    "RENTON": "RENT3.SA",
}
_preco_cache: dict = {}   # { ref: {"v": float|None, "ts": float} }
PRECO_TTL = 900           # 15 minutos

def _yf_ticker(ref: str) -> str:
    return TICKER_MAP_YF.get(ref, ref + ".SA")

def _ref_from_info(info: str | None) -> str | None:
    if not info:
        return None
    m = re.search(r"Ref:\s*([^\s|]+)", info)
    return m.group(1).strip() if m else None

def _yf_fetch_one(yft: str) -> float | None:
    """Busca preço de um único ticker via yfinance."""
    try:
        raw = yf.download(yft, period="5d", progress=False, auto_adjust=True)
        if raw.empty:
            return None
        col = raw["Close"].dropna()
        return round(float(col.iloc[-1]), 2) if not col.empty else None
    except Exception:
        return None

def buscar_precos(refs: list[str]) -> dict[str, float]:
    """Retorna {ref: preco} usando cache de 15 min. Busca via yfinance.
    Falhas NÃO são cacheadas — serão retentadas na próxima chamada.
    Se o batch falhar, tenta cada ticker individualmente.
    """
    agora = time.time()
    faltando = [r for r in set(refs)
                if r not in _preco_cache or (agora - _preco_cache[r]["ts"]) > PRECO_TTL]

    if faltando:
        tmap = {r: _yf_ticker(r) for r in faltando}
        uniq = list(set(tmap.values()))
        resultados: dict[str, float | None] = {}

        # 1ª tentativa: batch
        try:
            raw = yf.download(uniq, period="5d", progress=False, auto_adjust=True)
            if not raw.empty:
                if len(uniq) == 1:
                    col = raw["Close"].dropna()
                    v   = round(float(col.iloc[-1]), 2) if not col.empty else None
                    resultados[uniq[0]] = v
                else:
                    close = raw["Close"]
                    for yft in uniq:
                        try:
                            col = close[yft].dropna()
                            resultados[yft] = round(float(col.iloc[-1]), 2) if not col.empty else None
                        except Exception:
                            resultados[yft] = None
        except Exception as e:
            log.warning(f"yfinance batch erro: {e}")

        # 2ª tentativa: individual para os que falharam no batch
        falhou_batch = [yft for yft in uniq if resultados.get(yft) is None]
        if falhou_batch:
            log.info(f"yfinance retry individual: {falhou_batch}")
            for yft in falhou_batch:
                resultados[yft] = _yf_fetch_one(yft)

        # Cacheia apenas sucessos; falhas serão retentadas na próxima chamada
        for ref, yft in tmap.items():
            v = resultados.get(yft)
            if v is not None:
                _preco_cache[ref] = {"v": v, "ts": agora}
            else:
                log.warning(f"Preço não obtido para {ref} ({yft}) — não cacheando falha")

    return {r: _preco_cache[r]["v"] for r in set(refs)
            if _preco_cache.get(r, {}).get("v") is not None}


# ── Cache simples em memória ──────────────────────────────────────────────────
# Evita consultar o BigQuery a cada requisição.
# TTL padrão: 60 minutos (os dados chegam via webhook 1x por dia).
_cache: dict = {}
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", 3600))  # 1 hora

def cache_get(key: str):
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"]) < CACHE_TTL:
        log.info(f"Cache HIT: {key}")
        return entry["data"]
    return None

def cache_set(key: str, data):
    _cache[key] = {"data": data, "ts": time.time()}
    log.info(f"Cache SET: {key} ({len(str(data))} bytes)")

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="API Creta Capital", version="1.0.0")

# CORS — permite chamadas do portal (localhost em dev, GitHub Pages em prod)
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:8080,http://127.0.0.1:8080"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# ── Emails de admin (fallback caso Firestore/claims não estejam configurados) ──
ADMIN_EMAILS = {"fillipemsousa@gmail.com", "manu.lombardi@cretainvestimentos.com.br"}

# ── Firestore client (lazy, para buscar perfil quando claims não existem) ──────
_fs = None
def get_fs():
    global _fs
    if _fs is None:
        _fs = fb_firestore.client()
    return _fs

# ── Auth: verifica Bearer token do Firebase e retorna (role, assessor_name) ───
async def verificar_token(authorization: Optional[str] = None) -> dict:
    """
    Extrai e valida o Firebase ID token do header Authorization.
    Retorna dict com uid, email, role, assessor_name.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autenticação ausente ou inválido."
        )
    token = authorization.split(" ", 1)[1]
    try:
        decoded = firebase_auth.verify_id_token(token)
    except Exception as e:
        log.warning(f"Token inválido: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado ou inválido. Faça login novamente."
        )

    uid   = decoded.get("uid", "")
    email = decoded.get("email", "")

    # 1ª opção: usar custom claims (definidos em criar_usuarios.py)
    role          = decoded.get("role")
    assessor_name = decoded.get("assessor_name")

    # 2ª opção: buscar no Firestore (usuários que não passaram pelo script)
    if not role:
        try:
            doc = get_fs().collection("users").document(uid).get()
            if doc.exists:
                perfil = doc.to_dict()
                role          = perfil.get("role")
                assessor_name = perfil.get("assessor_name")
        except Exception as fe:
            log.warning(f"Não foi possível ler perfil Firestore: {fe}")

    # 3ª opção: fallback por e-mail (contas antigas / dev)
    if not role:
        role = "admin" if email in ADMIN_EMAILS else "assessor"

    decoded["role"]          = role
    decoded["assessor_name"] = assessor_name
    return decoded

# ── Helpers SQL ───────────────────────────────────────────────────────────────
def where_periodo(periodo: str, start_date: str = None, end_date: str = None) -> str:
    """Gera cláusula WHERE para o filtro de período."""
    if periodo == "custom" and start_date:
        fim = f"DATE '{end_date}'" if end_date else "CURRENT_DATE()"
        return f"DATE(Data_De_Referencia) >= DATE '{start_date}' AND DATE(Data_De_Referencia) <= {fim}"
    mapa = {
        "3m":  "DATE_SUB(CURRENT_DATE(), INTERVAL 3  MONTH)",
        "6m":  "DATE_SUB(CURRENT_DATE(), INTERVAL 6  MONTH)",
        "12m": "DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)",
        "ytd": "DATE_TRUNC(CURRENT_DATE(), YEAR)",
        "all": "DATE '2000-01-01'",
    }
    data_inicio = mapa.get(periodo, mapa["12m"])
    return f"DATE(Data_De_Referencia) >= {data_inicio}"

def run_query(sql: str) -> list[dict]:
    """Executa uma query no BigQuery e retorna lista de dicts."""
    log.info(f"BQ query: {sql[:120]}...")
    rows = bq.query(sql).result()
    return [dict(row) for row in rows]

def _mapa_assessor_sq() -> str:
    """
    Subquery SQL que devolve (Conta INT64, Assessor STRING) para todas as contas.
    Prioridade: exceções ativas (conta_assessor_excecoes) >
                base mensal mais recente (conta_assessor_base).
    Pode ser usada inline como subquery em qualquer JOIN ou WHERE.
    """
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

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/receitas")
async def receitas(
    periodo: str = "12m",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    """
    Retorna todos os dados da página Receitas e Repasses em uma única chamada.
    O frontend recebe um único JSON e monta todos os gráficos e tabelas.

    Parâmetro:
      periodo: "3m" | "6m" | "12m" | "ytd" | "all"  (default: "12m")
    """
    token_data = await verificar_token(authorization)
    role          = token_data.get("role", "assessor")
    assessor_name = token_data.get("assessor_name")

    # Admins veem tudo; assessores veem apenas seus próprios dados
    is_admin        = role == "admin"
    forced_assessor = None if is_admin else assessor_name

    cache_key = f"receitas:{periodo}:{start_date or ''}:{end_date or ''}:{forced_assessor or 'all'}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    where = where_periodo(periodo, start_date, end_date)

    # Filtro extra para não-admins
    qp: list = []
    if forced_assessor:
        where += " AND UPPER(TRIM(Assessor_Manual)) = UPPER(@assessor)"
        qp.append(ScalarQueryParameter("assessor", "STRING", forced_assessor.strip()))

    def run_q(sql: str) -> list[dict]:
        if qp:
            rows = bq.query(sql, job_config=QueryJobConfig(query_parameters=qp)).result()
        else:
            rows = bq.query(sql).result()
        log.info(f"BQ query: {sql[:100]}...")
        return [dict(r) for r in rows]

    # 1. KPIs — totais do período
    kpis = run_q(f"""
        SELECT
          ROUND(SUM(Comissao),             2) AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),     2) AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido),2) AS repasse_liquido,
          ROUND(SUM(Comissao_Liquida) - SUM(Repasse_Total_liquido), 2) AS receita_escritorio
        FROM {TABLE}
        WHERE {where}
    """)[0]

    # 2. Evolução mensal — barra principal
    evolucao = run_q(f"""
        SELECT
          FORMAT_DATE('%b/%y', DATE(Data_De_Referencia))      AS mes,
          DATE_TRUNC(DATE(Data_De_Referencia), MONTH)         AS data_ref,
          ROUND(SUM(Comissao),              2)                 AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),      2)                 AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido), 2)                 AS repasse_liquido,
          ROUND(SUM(Comissao_Liquida) - SUM(Repasse_Total_liquido), 2) AS receita_escritorio
        FROM {TABLE}
        WHERE {where}
        GROUP BY mes, data_ref
        ORDER BY data_ref ASC
    """)
    # Converte data_ref para string para serialização JSON
    for row in evolucao:
        if hasattr(row.get("data_ref"), "isoformat"):
            row["data_ref"] = row["data_ref"].isoformat()

    # 3. Por assessor — donuts + tabela
    por_assessor = run_q(f"""
        SELECT
          UPPER(COALESCE(Assessor_Manual, 'Sem assessor')) AS assessor,
          ROUND(SUM(Comissao),              2)       AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),      2)       AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido), 2)       AS repasse_liquido
        FROM {TABLE}
        WHERE {where}
          AND Assessor_Manual IS NOT NULL
        GROUP BY assessor
        ORDER BY repasse_liquido DESC
    """)

    # 4. Por categoria — tabela
    por_categoria = run_q(f"""
        SELECT
          COALESCE(Categoria_de_Repasse, 'Outros') AS categoria,
          ROUND(SUM(Comissao),               2)    AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),       2)    AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido),  2)    AS repasse
        FROM {TABLE}
        WHERE {where}
        GROUP BY categoria
        ORDER BY repasse DESC
    """)

    # 5. Por cliente — tabela (top 100 por repasse)
    por_cliente = run_q(f"""
        SELECT
          UPPER(COALESCE(Assessor_Manual, '')) AS assessor,
          COALESCE(Cliente, '')         AS cliente,
          ROUND(SUM(Receita_Bruta),              2) AS receita_bruta,
          ROUND(SUM(Receita_Liquida),            2) AS receita_liquida,
          ROUND(SUM(Comissao),                   2) AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),           2) AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido),      2) AS repasse
        FROM {TABLE}
        WHERE {where}
          AND Cliente IS NOT NULL
          AND Cliente != ''
        GROUP BY assessor, cliente
        ORDER BY repasse DESC
        LIMIT 100
    """)

    # 6. Evolução mensal por assessor (usada quando assessor/cliente está filtrado no frontend)
    evolucao_por_assessor = run_q(f"""
        SELECT
          UPPER(COALESCE(Assessor_Manual, '')) AS assessor,
          FORMAT_DATE('%b/%y', DATE(Data_De_Referencia))      AS mes,
          DATE_TRUNC(DATE(Data_De_Referencia), MONTH)         AS data_ref,
          ROUND(SUM(Comissao),              2)                 AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),      2)                 AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido), 2)                 AS repasse_liquido,
          ROUND(SUM(Comissao_Liquida) - SUM(Repasse_Total_liquido), 2) AS receita_escritorio
        FROM {TABLE}
        WHERE {where}
          AND Assessor_Manual IS NOT NULL
        GROUP BY assessor, mes, data_ref
        ORDER BY assessor, data_ref ASC
    """)
    for row in evolucao_por_assessor:
        if hasattr(row.get("data_ref"), "isoformat"):
            row["data_ref"] = row["data_ref"].isoformat()

    # 7. Por categoria por assessor (usada quando assessor/cliente está filtrado no frontend)
    por_assessor_categoria = run_q(f"""
        SELECT
          UPPER(COALESCE(Assessor_Manual, '')) AS assessor,
          COALESCE(Categoria_de_Repasse, 'Outros') AS categoria,
          ROUND(SUM(Comissao),              2) AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),      2) AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido), 2) AS repasse
        FROM {TABLE}
        WHERE {where}
          AND Assessor_Manual IS NOT NULL
        GROUP BY assessor, categoria
        ORDER BY assessor, repasse DESC
    """)

    resultado = {
        "periodo":               periodo,
        "gerado_em":             datetime.utcnow().isoformat(),
        "kpis":                  kpis,
        "evolucao":              evolucao,
        "por_assessor":          por_assessor,
        "por_categoria":         por_categoria,
        "por_cliente":           por_cliente,
        "evolucao_por_assessor": evolucao_por_assessor,
        "por_assessor_categoria": por_assessor_categoria,
    }

    cache_set(cache_key, resultado)
    return resultado


@app.get("/api/detalhe")
async def detalhe(
    periodo: str = "12m",
    assessor: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    """
    Retorna dados linha a linha para o relatório de detalhe Excel.
    Parâmetros:
      periodo: "3m" | "6m" | "12m" | "ytd" | "all"
      assessor: nome exato do assessor (apenas admins podem especificar um diferente do seu)
    """
    token_data    = await verificar_token(authorization)
    role          = token_data.get("role", "assessor")
    assessor_name = token_data.get("assessor_name")
    is_admin      = role == "admin"

    # Não-admins só podem ver seus próprios dados
    if not is_admin:
        assessor = assessor_name  # ignora parâmetro da URL

    cache_key = f"detalhe:{periodo}:{assessor or 'todos'}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    where = where_periodo(periodo)
    query_params = []

    if assessor:
        where += " AND UPPER(TRIM(Assessor_Manual)) = UPPER(@assessor)"
        query_params.append(ScalarQueryParameter("assessor", "STRING", assessor.strip()))

    sql = f"""
        SELECT
          FORMAT_DATE('%Y-%m-%d', DATE(Data_Receita))      AS Data_Receita,
          CAST(Conta AS STRING)                              AS Conta,
          COALESCE(Cliente, '')                              AS Cliente,
          COALESCE(Categoria_de_Repasse, '')                AS Categoria,
          COALESCE(Produto, '')                              AS Produto,
          COALESCE(Ativo, '')                                AS Ativo,
          COALESCE(Tipo_Receita, '')                         AS Tipo_Receita,
          ROUND(COALESCE(Receita_Bruta, 0), 4)              AS Receita_Bruta,
          ROUND(COALESCE(Receita_Liquida, 0), 4)            AS Receita_Liquida,
          ROUND(COALESCE(Comissao, 0), 4)                   AS Comissao,
          ROUND(COALESCE(Taxa_de_Repasse, 0), 4)            AS Taxa_de_Repasse,
          ROUND(COALESCE(Comissao_Liquida, 0), 4)           AS Comissao_Liquida,
          ROUND(COALESCE(Repasse_Total_liquido, 0), 4)      AS Repasse_Total_liquido
        FROM {TABLE}
        WHERE {where}
        ORDER BY Data_De_Referencia DESC, Cliente
        LIMIT 10000
    """

    job_config = QueryJobConfig(query_parameters=query_params) if query_params else None
    log.info(f"BQ detalhe query: periodo={periodo}, assessor={assessor}")
    if job_config:
        rows = bq.query(sql, job_config=job_config).result()
    else:
        rows = bq.query(sql).result()

    resultado = {"rows": [dict(r) for r in rows]}
    cache_set(cache_key, resultado)
    return resultado


@app.get("/api/evolucao_cliente")
async def evolucao_cliente(
    periodo: str = "12m",
    cliente: str = "",
    authorization: Optional[str] = Header(default=None),
):
    """
    Retorna evolução mensal de um cliente específico.
    Chamado pelo frontend quando o usuário seleciona um cliente no filtro.
    """
    token_data    = await verificar_token(authorization)
    role          = token_data.get("role", "assessor")
    assessor_name = token_data.get("assessor_name")
    is_admin      = role == "admin"

    if not cliente:
        raise HTTPException(status_code=400, detail="Parâmetro 'cliente' é obrigatório.")

    where  = where_periodo(periodo)
    qp     = [ScalarQueryParameter("cliente", "STRING", cliente.strip())]
    where += " AND TRIM(Cliente) = @cliente"

    # Não-admins só podem consultar clientes do próprio assessor
    if not is_admin and assessor_name:
        where += " AND UPPER(TRIM(Assessor_Manual)) = UPPER(@assessor)"
        qp.append(ScalarQueryParameter("assessor", "STRING", assessor_name.strip()))

    sql = f"""
        SELECT
          FORMAT_DATE('%b/%y', DATE(Data_De_Referencia))      AS mes,
          DATE_TRUNC(DATE(Data_De_Referencia), MONTH)         AS data_ref,
          ROUND(SUM(Comissao),              2)                 AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),      2)                 AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido), 2)                 AS repasse_liquido,
          ROUND(SUM(Comissao_Liquida) - SUM(Repasse_Total_liquido), 2) AS receita_escritorio
        FROM {TABLE}
        WHERE {where}
        GROUP BY mes, data_ref
        ORDER BY data_ref ASC
    """
    sql_cat = f"""
        SELECT
          COALESCE(Categoria_de_Repasse, 'Outros') AS categoria,
          ROUND(SUM(Comissao),              2) AS comissao_bruta,
          ROUND(SUM(Comissao_Liquida),      2) AS comissao_liquida,
          ROUND(SUM(Repasse_Total_liquido), 2) AS repasse
        FROM {TABLE}
        WHERE {where}
        GROUP BY categoria
        ORDER BY repasse DESC
    """
    log.info(f"BQ evolucao_cliente: cliente={cliente!r}, periodo={periodo}")
    rows     = bq.query(sql,     job_config=QueryJobConfig(query_parameters=qp)).result()
    rows_cat = bq.query(sql_cat, job_config=QueryJobConfig(query_parameters=qp)).result()

    result = []
    for r in rows:
        row = dict(r)
        if hasattr(row.get("data_ref"), "isoformat"):
            row["data_ref"] = row["data_ref"].isoformat()
        result.append(row)

    return {"evolucao": result, "categorias": [dict(r) for r in rows_cat]}


@app.get("/api/posicoes")
async def posicoes_endpoint(
    authorization: Optional[str] = Header(default=None),
):
    """
    Retorna posições consolidadas por conta e classe, unindo:
      - posicao_das_contas   (data mais recente)
      - receitas_para_repasse (nome do cliente + assessor)
      - suitability_contas   (perfil de suitability mais recente)

    Admins veem todas as contas; assessores veem apenas suas próprias.
    """
    token_data    = await verificar_token(authorization)
    role          = token_data.get("role", "assessor")
    assessor_name = token_data.get("assessor_name")
    is_admin      = role == "admin"
    forced_assessor = None if is_admin else assessor_name

    cache_key = f"posicoes:{forced_assessor or 'all'}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    qp = []
    if forced_assessor:
        qp.append(ScalarQueryParameter("assessor", "STRING", forced_assessor.strip()))
        ma_join = f"INNER JOIN {_mapa_assessor_sq()} ma ON SAFE_CAST(TRIM(p.Conta) AS INT64) = ma.Conta AND ma.Assessor = @assessor"
    else:
        ma_join = f"LEFT JOIN {_mapa_assessor_sq()} ma ON SAFE_CAST(TRIM(p.Conta) AS INT64) = ma.Conta"

    sql = f"""
        WITH ultima_data AS (
            SELECT MAX(DATE(Data)) AS max_data
            FROM {TABLE_POSICAO}
        ),
        posicao_base AS (
            SELECT
                TRIM(p.Conta) AS Conta,
                p.Classe,
                ROUND(SUM(p.ValorBruto), 2) AS auc
            FROM {TABLE_POSICAO} p
            JOIN ultima_data ON DATE(p.Data) = ultima_data.max_data
            WHERE p.Classe != 'Aluguel de Ações'
            GROUP BY TRIM(p.Conta), p.Classe
        ),
        nomes AS (
            SELECT Conta AS conta_num, MAX(Cliente) AS cliente
            FROM {TABLE}
            WHERE Cliente IS NOT NULL AND Cliente != ''
            GROUP BY Conta
        ),
        suit AS (
            SELECT Conta, Perfil
            FROM {TABLE_SUITABILITY}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY Conta ORDER BY DataExtracao DESC) = 1
        )
        SELECT
            p.Conta,
            COALESCE(n.cliente,  '')            AS cliente,
            UPPER(COALESCE(ma.Assessor, ''))           AS assessor,
            COALESCE(suit.Perfil, 'Sem perfil') AS perfil,
            p.Classe,
            p.auc,
            ud.max_data AS data_referencia
        FROM posicao_base p
        CROSS JOIN ultima_data ud
        {ma_join}
        LEFT JOIN nomes n  ON SAFE_CAST(TRIM(p.Conta) AS INT64) = n.conta_num
        LEFT JOIN suit     ON p.Conta = suit.Conta
        ORDER BY COALESCE(n.cliente, p.Conta), p.Classe
    """

    log.info(f"BQ posicoes: assessor={forced_assessor or 'todos'}")
    if qp:
        rows_raw = list(bq.query(sql, job_config=QueryJobConfig(query_parameters=qp)).result())
    else:
        rows_raw = list(bq.query(sql).result())

    rows_list: list[dict] = []
    data_ref: Optional[str] = None
    por_classe: dict = {}

    for row in rows_raw:
        d = dict(row)
        dr = d.pop("data_referencia", None)
        if data_ref is None and dr is not None:
            data_ref = dr.isoformat() if hasattr(dr, "isoformat") else str(dr)
        classe = d.get("Classe") or "Outros"
        por_classe[classe] = round(por_classe.get(classe, 0) + (d.get("auc") or 0), 2)
        rows_list.append(d)

    por_classe_list = [
        {"classe": k, "auc": v}
        for k, v in sorted(por_classe.items(), key=lambda x: -x[1])
    ]

    resultado = {
        "data_referencia":   data_ref,
        "por_conta_classe":  rows_list,
        "por_classe":        por_classe_list,
    }
    cache_set(cache_key, resultado)
    return resultado


@app.get("/api/opcoes")
async def opcoes_endpoint(
    authorization: Optional[str] = Header(default=None),
):
    """
    Retorna todas as posições de derivativos (opções) da data mais recente.
    Admins veem todas as contas; assessores veem apenas as suas.
    """
    token_data    = await verificar_token(authorization)
    role          = token_data.get("role", "assessor")
    assessor_name = token_data.get("assessor_name")
    is_admin      = role == "admin"

    cache_key = f"opcoes:{assessor_name if not is_admin else 'all'}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    qp = []
    if not is_admin and assessor_name:
        qp.append(ScalarQueryParameter("assessor", "STRING", assessor_name.strip()))
        join_part = f"""
            INNER JOIN (
                SELECT CAST(Conta AS STRING) AS cnt
                FROM {_mapa_assessor_sq()}
                WHERE Assessor = @assessor
            ) ma ON TRIM(p.Conta) = ma.cnt
        """
    else:
        join_part = ""

    sql = f"""
        WITH ultima_data AS (
            SELECT MAX(DATE(Data)) AS max_data
            FROM {TABLE_POSICAO}
            WHERE Classe = 'Derivativo'
        ),
        clientes AS (
            SELECT
                Conta            AS conta_num,
                MAX(Cliente)     AS cliente
            FROM {TABLE}
            WHERE Cliente IS NOT NULL AND Cliente != ''
            GROUP BY Conta
        )
        SELECT
            p.Conta,
            COALESCE(c.cliente, '')  AS Cliente,
            p.Subclasse,
            p.Nome,
            p.Ticker,
            FORMAT_DATE('%Y-%m-%d', SAFE_CAST(p.Vencimento AS DATE)) AS Vencimento,
            p.Direcao,
            SAFE_CAST(p.Quantidade AS FLOAT64) AS Quantidade,
            SAFE_CAST(p.Preco      AS FLOAT64) AS Preco,
            SAFE_CAST(p.ValorBruto AS FLOAT64) AS ValorBruto,
            p.InfoExtra,
            ud.max_data AS data_referencia
        FROM {TABLE_POSICAO} p
        CROSS JOIN ultima_data ud
        LEFT JOIN clientes c ON SAFE_CAST(TRIM(p.Conta) AS INT64) = c.conta_num
        {join_part}
        WHERE DATE(p.Data) = ud.max_data
          AND p.Classe = 'Derivativo'
        ORDER BY p.Conta, SAFE_CAST(p.Vencimento AS DATE)
    """

    log.info(f"BQ opcoes: {'todos' if is_admin else assessor_name}")
    if qp:
        rows_raw = list(bq.query(sql, job_config=QueryJobConfig(query_parameters=qp)).result())
    else:
        rows_raw = list(bq.query(sql).result())

    data_ref = None
    rows_list = []
    for row in rows_raw:
        d = dict(row)
        dr = d.pop("data_referencia", None)
        if data_ref is None and dr is not None:
            data_ref = dr.isoformat() if hasattr(dr, "isoformat") else str(dr)
        for field in ["Quantidade", "Preco", "ValorBruto"]:
            if d.get(field) is not None:
                try:
                    d[field] = float(d[field])
                except (TypeError, ValueError):
                    d[field] = None
        rows_list.append(d)

    # Cotações dos ativos subjacentes (ex: BOVA11, VALE3, ITUB4)
    refs = list({_ref_from_info(r.get("InfoExtra")) for r in rows_list} - {None})
    precos = buscar_precos(refs) if refs else {}

    resultado = {"data_referencia": data_ref, "opcoes": rows_list, "precos": precos}
    cache_set(cache_key, resultado)
    return resultado


@app.get("/api/posicao")
async def posicao(
    authorization: Optional[str] = Header(default=None),
):
    """
    Retorna AUC total e contagem de contas ativas para a data mais recente
    da tabela posicao_das_contas.
    """
    await verificar_token(authorization)  # exige login; sem filtro por assessor (tabela não tem coluna Assessor)

    cache_key = "posicao:snapshot"
    cached = cache_get(cache_key)
    if cached:
        return cached

    sql = f"""
        WITH ultima_data AS (
            SELECT MAX(DATE(Data)) AS max_data
            FROM {TABLE_POSICAO}
        )
        SELECT
            ROUND(SUM(p.ValorBruto), 2)    AS auc_total,
            COUNT(DISTINCT p.Conta)         AS contas_ativas,
            ud.max_data                     AS data_referencia
        FROM {TABLE_POSICAO} p
        CROSS JOIN ultima_data ud
        WHERE DATE(p.Data) = ud.max_data
          AND p.Classe != 'Aluguel de Ações'
        GROUP BY ud.max_data
    """

    log.info("BQ posicao: buscando snapshot mais recente")
    rows = list(bq.query(sql).result())

    if not rows:
        resultado = {"auc_total": 0, "contas_ativas": 0, "data_referencia": None}
    else:
        row = rows[0]
        data_ref = row["data_referencia"]
        resultado = {
            "auc_total":       float(row["auc_total"]),
            "contas_ativas":   int(row["contas_ativas"]),
            "data_referencia": data_ref.date().isoformat() if hasattr(data_ref, "date") else str(data_ref),
        }

    cache_set(cache_key, resultado)
    return resultado


# ── Modelo para criação de exceção ───────────────────────────────────────────
class ExcecaoBody(BaseModel):
    conta:       int
    assessor:    str
    data_inicio: str            # "YYYY-MM-DD"
    data_fim:    Optional[str] = None   # "YYYY-MM-DD" ou None
    motivo:      Optional[str] = None


@app.get("/api/config/excecoes")
async def listar_excecoes(
    authorization: Optional[str] = Header(default=None),
):
    """Lista todas as exceções de assessor — apenas admins."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    sql = f"""
        SELECT
            Conta,
            Assessor,
            FORMAT_DATE('%Y-%m-%d', DataInicio) AS DataInicio,
            FORMAT_DATE('%Y-%m-%d', DataFim)    AS DataFim,
            COALESCE(Motivo, '')                AS Motivo,
            FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', DataCriacao) AS DataCriacao,
            COALESCE(CriadoPor, '')             AS CriadoPor
        FROM {TABLE_EXCECOES}
        ORDER BY DataInicio DESC
    """
    rows = run_query(sql)
    return {"excecoes": rows}


@app.post("/api/config/excecoes")
async def criar_excecao(
    body: ExcecaoBody,
    authorization: Optional[str] = Header(default=None),
):
    """Cria uma nova exceção de assessor — apenas admins."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    email = token_data.get("email", "")

    data_fim_expr = f"DATE('{body.data_fim}')" if body.data_fim else "NULL"
    motivo_expr   = f"'{body.motivo.replace(chr(39), chr(39)*2)}'" if body.motivo else "NULL"
    assessor_esc  = body.assessor.replace("'", "''")

    sql = f"""
        INSERT INTO {TABLE_EXCECOES}
            (Conta, Assessor, DataInicio, DataFim, Motivo, DataCriacao, CriadoPor)
        VALUES
            ({body.conta}, '{assessor_esc}', DATE('{body.data_inicio}'),
             {data_fim_expr}, {motivo_expr},
             CURRENT_DATETIME(), '{email}')
    """
    try:
        bq.query(sql).result()
    except Exception as e:
        log.error(f"Erro ao criar exceção: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # Invalida cache relacionado (exceções afetam posicoes, relatorio, assessores e opcoes)
    for k in list(_cache.keys()):
        if any(x in k for x in ("posicoes", "receitas", "relatorio", "assessores", "opcoes")):
            del _cache[k]

    return {"ok": True}


@app.delete("/api/config/excecoes")
async def deletar_excecao(
    conta:       int = Query(...),
    data_inicio: str = Query(...),   # "YYYY-MM-DD"
    authorization: Optional[str] = Header(default=None),
):
    """Remove uma exceção pelo par (Conta, DataInicio) — apenas admins."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    sql = f"""
        DELETE FROM {TABLE_EXCECOES}
        WHERE Conta = {conta}
          AND DataInicio = DATE('{data_inicio}')
    """
    try:
        bq.query(sql).result()
    except Exception as e:
        log.error(f"Erro ao deletar exceção: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    for k in list(_cache.keys()):
        if "posicoes" in k or "receitas" in k:
            del _cache[k]

    return {"ok": True}


# ── Pipeline: gestão de arquivos no GCS ──────────────────────────────────────

@app.get("/api/pipeline/arquivos")
async def listar_arquivos(
    authorization: Optional[str] = Header(default=None),
):
    """Lista arquivos na pasta entradas/ do bucket — apenas admins."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    bucket = gcs_client.bucket(GCS_BUCKET)
    blobs  = bucket.list_blobs(prefix=GCS_PREFIX)

    arquivos = []
    for blob in blobs:
        nome = blob.name.replace(GCS_PREFIX, "")
        if not nome:
            continue
        arquivos.append({
            "nome":        nome,
            "tamanho_kb":  round(blob.size / 1024, 1),
            "atualizado":  blob.updated.isoformat() if blob.updated else None,
        })

    arquivos.sort(key=lambda x: x["nome"])
    return {"arquivos": arquivos}


@app.post("/api/pipeline/upload")
async def upload_arquivo(
    file: UploadFile = File(...),
    authorization: Optional[str] = Header(default=None),
):
    """Faz upload de um arquivo para entradas/ no bucket — apenas admins."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Apenas arquivos .xlsx são aceitos.")

    try:
        bucket = gcs_client.bucket(GCS_BUCKET)
        blob   = bucket.blob(f"{GCS_PREFIX}{file.filename}")
        blob.upload_from_file(file.file, content_type=file.content_type or "application/octet-stream")
        log.info(f"Upload GCS: {file.filename}")
        return {"ok": True, "nome": file.filename}
    except Exception as e:
        log.error(f"Erro upload GCS: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/pipeline/arquivo")
async def deletar_arquivo(
    nome: str = Query(...),
    authorization: Optional[str] = Header(default=None),
):
    """Remove um arquivo de entradas/ no bucket — apenas admins."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    try:
        bucket = gcs_client.bucket(GCS_BUCKET)
        blob   = bucket.blob(f"{GCS_PREFIX}{nome}")
        blob.delete()
        log.info(f"Deletado GCS: {nome}")
        return {"ok": True}
    except Exception as e:
        log.error(f"Erro delete GCS: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pipeline/arquivo/download")
async def download_arquivo(
    nome: str = Query(...),
    authorization: Optional[str] = Header(default=None),
):
    """Faz download de um arquivo do bucket — apenas admins."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    try:
        bucket = gcs_client.bucket(GCS_BUCKET)
        blob   = bucket.blob(f"{GCS_PREFIX}{nome}")
        buffer = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(nome)}"},
        )
    except Exception as e:
        log.error(f"Erro download GCS: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/config/excecoes/excel")
async def excecoes_excel(
    authorization: Optional[str] = Header(default=None),
):
    """Exporta todas as exceções como arquivo Excel — apenas admins."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    sql = f"""
        SELECT
            Conta,
            Assessor,
            FORMAT_DATE('%d/%m/%Y', DataInicio) AS DataInicio,
            FORMAT_DATE('%d/%m/%Y', DataFim)    AS DataFim,
            COALESCE(Motivo, '')                AS Motivo,
            COALESCE(CriadoPor, '')             AS CriadoPor,
            FORMAT_DATETIME('%d/%m/%Y %H:%M', DataCriacao) AS DataCriacao
        FROM {TABLE_EXCECOES}
        ORDER BY DataInicio DESC
    """
    rows = run_query(sql)
    df   = pd.DataFrame(rows)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Exceções")
        ws = writer.sheets["Exceções"]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col) + 4
            ws.column_dimensions[col[0].column_letter].width = min(max_len, 50)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="excecoes_assessor.xlsx"'},
    )


# ── Relatórios ───────────────────────────────────────────────────────────────

@app.get("/api/assessores")
async def assessores_endpoint(
    authorization: Optional[str] = Header(default=None),
):
    """Lista assessores distintos — apenas admins (filtro do relatório)."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Apenas administradores.")

    cached = cache_get("assessores")
    if cached:
        return cached

    sql = f"""
        SELECT DISTINCT assessor
        FROM (
            SELECT UPPER(Assessor) AS assessor
            FROM {TABLE_ASSESSOR_BASE}
            WHERE MesRef = (SELECT MAX(MesRef) FROM {TABLE_ASSESSOR_BASE})
            UNION DISTINCT
            SELECT UPPER(Assessor) AS assessor
            FROM {TABLE_EXCECOES}
            WHERE DataInicio <= CURRENT_DATE()
              AND (DataFim IS NULL OR DataFim >= CURRENT_DATE())
        )
        WHERE assessor IS NOT NULL AND assessor != ''
        ORDER BY assessor
    """
    rows = run_query(sql)
    resultado = {"assessores": [r["assessor"] for r in rows]}
    cache_set("assessores", resultado)
    return resultado


@app.get("/api/relatorio/historico")
async def relatorio_historico(
    periodo: str = "12m",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    assessor: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    """
    Relatório histórico: AuC diário, novas contas/mês, receita+ROA mensal,
    top 20 variações absolutas de PL (últimos 30 dias).

    periodo: "3m" | "6m" | "12m" | "all"
    assessor: nome do assessor (apenas admins podem especificar)
    """
    token_data    = await verificar_token(authorization)
    role          = token_data.get("role", "assessor")
    assessor_name = token_data.get("assessor_name")
    is_admin      = role == "admin"

    filter_assessor = (assessor.strip() if assessor else None) if is_admin else assessor_name

    cache_key = f"relatorio:{periodo}:{start_date or ''}:{end_date or ''}:{filter_assessor or 'all'}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    mapa_periodo = {
        "3m":  "DATE_SUB(CURRENT_DATE(), INTERVAL 3  MONTH)",
        "6m":  "DATE_SUB(CURRENT_DATE(), INTERVAL 6  MONTH)",
        "12m": "DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)",
        "all": "DATE '2010-01-01'",
    }
    if periodo == "custom" and start_date:
        data_inicio = f"DATE '{start_date}'"
        data_fim    = f"DATE '{end_date}'" if end_date else "CURRENT_DATE()"
    else:
        data_inicio = mapa_periodo.get(periodo, mapa_periodo["12m"])
        data_fim    = "CURRENT_DATE()"

    qp = [ScalarQueryParameter("assessor", "STRING", filter_assessor)] if filter_assessor else []

    # CTEs que filtram posicao_das_contas por assessor (via conta_assessor_base + exceções)
    if filter_assessor:
        contas_cte = f"""
    contas_assessor AS (
        SELECT Conta AS conta_num
        FROM {_mapa_assessor_sq()}
        WHERE Assessor = @assessor
    ),"""
        contas_join = "INNER JOIN contas_assessor ca ON SAFE_CAST(TRIM(p.Conta) AS INT64) = ca.conta_num"
    else:
        contas_cte  = ""
        contas_join = ""

    assessor_where = "AND UPPER(TRIM(Assessor_Manual)) = UPPER(@assessor)" if filter_assessor else ""

    def rq(sql: str) -> list[dict]:
        if qp:
            return [dict(r) for r in bq.query(sql, job_config=QueryJobConfig(query_parameters=qp)).result()]
        return [dict(r) for r in bq.query(sql).result()]

    def ser(rows: list[dict]) -> list[dict]:
        return [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()} for row in rows]

    # ── 1. AuC diário ─────────────────────────────────────────────────────────
    sql_auc = f"""
        WITH{contas_cte}
        auc_dia AS (
            SELECT DATE(p.Data) AS dia, ROUND(SUM(p.ValorBruto), 2) AS auc
            FROM {TABLE_POSICAO} p
            {contas_join}
            WHERE DATE(p.Data) >= {data_inicio}
              AND DATE(p.Data) <= {data_fim}
              AND p.Classe != 'Aluguel de Ações'
            GROUP BY DATE(p.Data)
        )
        SELECT FORMAT_DATE('%Y-%m-%d', dia) AS dia, auc
        FROM auc_dia
        ORDER BY dia
    """

    # ── 2. Novas contas por mês (primeira aparição na posicao_das_contas) ───────
    sql_novas = f"""
        WITH{contas_cte}
        primeira_aparicao AS (
            SELECT TRIM(p.Conta) AS Conta, MIN(DATE(p.Data)) AS primeira_data
            FROM {TABLE_POSICAO} p
            {contas_join}
            GROUP BY TRIM(p.Conta)
        )
        SELECT
            FORMAT_DATE('%Y-%m', DATE_TRUNC(primeira_data, MONTH)) AS mes,
            COUNT(*) AS novas_contas
        FROM primeira_aparicao
        WHERE primeira_data >= {data_inicio}
          AND primeira_data <= {data_fim}
        GROUP BY mes
        ORDER BY mes
    """

    # ── 3. Receita + ROA mensal ────────────────────────────────────────────────
    sql_receita = f"""
        WITH receita_mensal AS (
            SELECT
                FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(Data_De_Referencia), MONTH)) AS mes,
                ROUND(SUM(Receita_Liquida),       2) AS receita_liquida,
                ROUND(SUM(Receita_Bruta),         2) AS receita_bruta,
                ROUND(SUM(Comissao_Liquida),      2) AS comissao_liquida,
                ROUND(SUM(Repasse_Total_liquido), 2) AS repasse_liquido
            FROM {TABLE}
            WHERE DATE(Data_De_Referencia) >= {data_inicio}
              AND DATE(Data_De_Referencia) <= {data_fim}
              {assessor_where}
            GROUP BY mes
        ),
        auc_mensal AS (
            SELECT
                FORMAT_DATE('%Y-%m', DATE_TRUNC(p.dia_data, MONTH)) AS mes,
                ROUND(AVG(p.auc_dia), 2) AS avg_auc
            FROM (
                SELECT DATE(p2.Data) AS dia_data, SUM(p2.ValorBruto) AS auc_dia
                FROM {TABLE_POSICAO} p2
                WHERE DATE(p2.Data) >= {data_inicio}
                  AND DATE(p2.Data) <= {data_fim}
                  AND p2.Classe != 'Aluguel de Ações'
                GROUP BY DATE(p2.Data)
            ) p
            GROUP BY mes
        )
        SELECT
            r.mes,
            r.receita_liquida,
            r.receita_bruta,
            r.comissao_liquida,
            r.repasse_liquido,
            ROUND(COALESCE(a.avg_auc, 0), 0) AS avg_auc,
            CASE WHEN COALESCE(a.avg_auc, 0) > 0
                 THEN ROUND((r.receita_bruta / a.avg_auc) * 12 * 100, 4)
                 ELSE NULL
            END AS roa_anualizado_pct
        FROM receita_mensal r
        LEFT JOIN auc_mensal a USING (mes)
        ORDER BY r.mes
    """

    # ── 4. Top 20 PL movers (delta AuC vs ~30 dias atrás) ────────────────────
    sql_movers = f"""
        WITH ultima_data AS (
            SELECT MAX(DATE(Data)) AS max_data
            FROM {TABLE_POSICAO}
        ),
        data_ref AS (
            SELECT MAX(DATE(p.Data)) AS ref_data
            FROM {TABLE_POSICAO} p
            CROSS JOIN ultima_data ud
            WHERE DATE(p.Data) <= DATE_SUB(ud.max_data, INTERVAL 30 DAY)
        ),{contas_cte}
        auc_atual AS (
            SELECT TRIM(p.Conta) AS Conta, ROUND(SUM(p.ValorBruto), 2) AS auc
            FROM {TABLE_POSICAO} p
            CROSS JOIN ultima_data ud
            {contas_join}
            WHERE DATE(p.Data) = ud.max_data
              AND p.Classe != 'Aluguel de Ações'
            GROUP BY TRIM(p.Conta)
        ),
        auc_ant AS (
            SELECT TRIM(p.Conta) AS Conta, ROUND(SUM(p.ValorBruto), 2) AS auc
            FROM {TABLE_POSICAO} p
            CROSS JOIN data_ref dr
            {contas_join}
            WHERE DATE(p.Data) = dr.ref_data
              AND p.Classe != 'Aluguel de Ações'
            GROUP BY TRIM(p.Conta)
        ),
        nomes AS (
            SELECT Conta AS conta_num, MAX(Cliente) AS cliente
            FROM {TABLE}
            WHERE Cliente IS NOT NULL AND Cliente != ''
            GROUP BY Conta
        )
        SELECT
            a.Conta,
            COALESCE(n.cliente,   '') AS cliente,
            UPPER(COALESCE(ma.Assessor, '')) AS assessor,
            COALESCE(a.auc, 0)        AS auc_atual,
            COALESCE(r.auc, 0)        AS auc_ref,
            ROUND(COALESCE(a.auc, 0) - COALESCE(r.auc, 0), 2) AS delta_auc
        FROM auc_atual a
        LEFT JOIN auc_ant r ON a.Conta = r.Conta
        LEFT JOIN nomes n   ON SAFE_CAST(a.Conta AS INT64) = n.conta_num
        LEFT JOIN {_mapa_assessor_sq()} ma ON SAFE_CAST(a.Conta AS INT64) = ma.Conta
        ORDER BY ABS(COALESCE(a.auc, 0) - COALESCE(r.auc, 0)) DESC
        LIMIT 20
    """

    # ── 5. Top 20 Categoria × Cliente por Receita Bruta ─────────────────────
    sql_top_sub = f"""
        SELECT
            COALESCE(NULLIF(TRIM(Categoria), ''), 'Sem Categoria') AS subclasse,
            COALESCE(NULLIF(TRIM(Produto),   ''), '—')             AS produto,
            CAST(Conta AS STRING)                                   AS Conta,
            COALESCE(NULLIF(TRIM(Cliente), ''), '—')               AS cliente,
            UPPER(COALESCE(TRIM(Assessor_Manual), ''))             AS assessor,
            ROUND(SUM(Receita_Bruta),         2) AS receita_bruta,
            ROUND(SUM(Receita_Liquida),       2) AS receita_liquida,
            ROUND(SUM(Comissao_Liquida),      2) AS comissao_liquida,
            ROUND(SUM(Repasse_Total_liquido), 2) AS repasse
        FROM {TABLE}
        WHERE DATE(Data_De_Referencia) >= {data_inicio}
          AND DATE(Data_De_Referencia) <= {data_fim}
          {assessor_where}
          AND Categoria IS NOT NULL AND TRIM(Categoria) != ''
        GROUP BY subclasse, produto, Conta, cliente, assessor
        ORDER BY receita_bruta DESC
        LIMIT 20
    """

    log.info(f"BQ relatorio/historico: periodo={periodo}, assessor={filter_assessor or 'todos'}")
    auc_diario   = ser(rq(sql_auc))
    novas_contas = ser(rq(sql_novas))
    receita_roa  = ser(rq(sql_receita))
    pl_movers    = ser(rq(sql_movers))
    top_subclass = ser(rq(sql_top_sub))

    # KPIs derivados
    auc_atual_val     = float(auc_diario[-1]["auc"]) if auc_diario else 0
    delta_auc         = round(
        float(auc_diario[-1]["auc"]) - float(auc_diario[0]["auc"]), 2
    ) if len(auc_diario) >= 2 else 0
    novas_total       = sum(int(r.get("novas_contas") or 0) for r in novas_contas)
    receita_total     = round(sum(float(r.get("receita_liquida") or 0) for r in receita_roa), 2)
    roa_ultimo        = float(receita_roa[-1]["roa_anualizado_pct"]) if receita_roa and receita_roa[-1].get("roa_anualizado_pct") is not None else None

    resultado = {
        "periodo":      periodo,
        "assessor":     filter_assessor,
        "gerado_em":    datetime.utcnow().isoformat(),
        "kpis": {
            "auc_atual":            auc_atual_val,
            "delta_auc_periodo":    delta_auc,
            "novas_contas_periodo": novas_total,
            "roa_anualizado_pct":   roa_ultimo,
            "receita_periodo":      receita_total,
        },
        "auc_diario":   auc_diario,
        "novas_contas": novas_contas,
        "receita_roa":  receita_roa,
        "pl_movers":    pl_movers,
        "top_subclass": top_subclass,
    }

    cache_set(cache_key, resultado)
    return resultado


@app.delete("/api/cache")
async def limpar_cache(
    authorization: Optional[str] = Header(default=None),
):
    """Limpa o cache manualmente — apenas admins. Útil após carga de novos dados."""
    token_data = await verificar_token(authorization)
    if token_data.get("role") != "admin" and token_data.get("email") not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Apenas administradores podem limpar o cache.")
    _cache.clear()
    return {"ok": True, "message": "Cache limpo."}
