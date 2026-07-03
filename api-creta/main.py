"""
api-creta — Cloud Run service
Serve dados do BigQuery para o Portal Creta.

Endpoints:
  GET /api/receitas?periodo=12m   → todos os dados da página Receitas e Repasses
  GET /health                     → health check
"""

import os
import time
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Header, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
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
TABLE_POSICAO    = f"`{GCP_PROJECT}.{DATASET}.posicao_das_contas`"
TABLE_SUITABILITY = f"`{GCP_PROJECT}.{DATASET}.suitability_contas`"

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
    allow_methods=["GET"],
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
def where_periodo(periodo: str) -> str:
    """Gera cláusula WHERE para o filtro de período."""
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

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/receitas")
async def receitas(
    periodo: str = "12m",
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

    cache_key = f"receitas:{periodo}:{forced_assessor or 'all'}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    where = where_periodo(periodo)

    # Filtro extra para não-admins
    qp: list = []
    if forced_assessor:
        where += " AND TRIM(Assessor_Manual) = @assessor"
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
          COALESCE(Assessor_Manual, 'Sem assessor') AS assessor,
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
          COALESCE(Assessor_Manual, '') AS assessor,
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
          COALESCE(Assessor_Manual, '') AS assessor,
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
          COALESCE(Assessor_Manual, '') AS assessor,
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
        where += " AND TRIM(Assessor_Manual) = @assessor"
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
        where += " AND TRIM(Assessor_Manual) = @assessor"
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
    assessor_filter = ""
    join_type = "LEFT"
    if forced_assessor:
        assessor_filter = "AND TRIM(Assessor_Manual) = @assessor"
        qp.append(ScalarQueryParameter("assessor", "STRING", forced_assessor.strip()))
        join_type = "INNER"  # só contas do assessor aparecem

    sql = f"""
        WITH ultima_data AS (
            SELECT MAX(DATE(Data)) AS max_data
            FROM {TABLE_POSICAO}
        ),
        posicao_base AS (
            SELECT
                TRIM(p.Conta)  AS Conta,
                p.Classe,
                ROUND(SUM(p.ValorBruto), 2) AS auc
            FROM {TABLE_POSICAO} p
            JOIN ultima_data ON DATE(p.Data) = ultima_data.max_data
            GROUP BY TRIM(p.Conta), p.Classe
        ),
        clientes AS (
            SELECT
                Conta AS conta_num,  -- INT64, usado no JOIN
                MAX(Cliente)         AS cliente,
                MAX(Assessor_Manual) AS assessor
            FROM {TABLE}
            WHERE Cliente IS NOT NULL AND Cliente != ''
            {assessor_filter}
            GROUP BY Conta
        ),
        suit AS (
            SELECT Conta, Perfil
            FROM {TABLE_SUITABILITY}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY Conta ORDER BY DataExtracao DESC) = 1
        )
        SELECT
            p.Conta,
            COALESCE(c.cliente,  '')           AS cliente,
            COALESCE(c.assessor, '')           AS assessor,
            COALESCE(suit.Perfil, 'Sem perfil') AS perfil,
            p.Classe,
            p.auc,
            ud.max_data AS data_referencia
        FROM posicao_base p
        CROSS JOIN ultima_data ud
        {join_type} JOIN clientes c  ON SAFE_CAST(TRIM(p.Conta) AS INT64) = c.conta_num
        LEFT  JOIN suit             ON p.Conta = suit.Conta
        ORDER BY COALESCE(c.cliente, p.Conta), p.Classe
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
