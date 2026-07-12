"""
job-noticias — Cloud Run Job
Busca notícias financeiras em português via NewsAPI e salva no Firestore.
Roda 2x por dia: 08h (manha) e 14h (tarde) — determina o período pelo horário.

Firestore: noticias/manha  e  noticias/tarde
"""

import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
from google.cloud import firestore

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("job-noticias")

# ── Config ────────────────────────────────────────────────────────────────────
NEWSAPI_KEY = os.environ["NEWSAPI_KEY"]
FS_PROJECT  = os.getenv("FS_PROJECT", "creta-btg-bd3a8")
MAX_NOTICIAS = 5

NEWSAPI_URL = "https://newsapi.org/v2/everything"

# ── Categorias e queries ──────────────────────────────────────────────────────
# Ordem define a prioridade quando há muitas notícias disponíveis.
CATEGORIAS = [
    {
        "nome": "Renda Variável",
        "slug": "rv",
        "query": 'Ibovespa OR "bolsa de valores" OR B3',
    },
    {
        "nome": "Renda Fixa",
        "slug": "rf",
        "query": '"renda fixa" OR "tesouro direto" OR CDB OR LCI',
    },
    {
        "nome": "Macro",
        "slug": "macro",
        "query": 'PIB OR "banco central" OR "economia brasileira" OR "crescimento econômico"',
    },
    {
        "nome": "Curva de Juros",
        "slug": "juros",
        "query": '"curva de juros" OR "juros futuros" OR "DI futuro" OR Selic',
    },
    {
        "nome": "IPCA",
        "slug": "ipca",
        "query": 'IPCA OR inflação OR INPC',
    },
    {
        "nome": "Internacional",
        "slug": "internacional",
        "query": '"mercados internacionais" OR "wall street" OR "fed" OR dólar',
    },
]


# ── NewsAPI ───────────────────────────────────────────────────────────────────

def buscar_noticia(categoria: dict, urls_vistos: set) -> dict | None:
    """
    Busca a notícia mais recente da categoria.
    Pula artigos removidos, sem descrição, ou já encontrados em outra categoria.
    Retorna None se não encontrar nada útil.
    """
    try:
        resp = requests.get(
            NEWSAPI_URL,
            params={
                "q":        categoria["query"],
                "language": "pt",
                "sortBy":   "publishedAt",
                "pageSize": 5,   # pede 5 para ter margem de filtragem
                "apiKey":   NEWSAPI_KEY,
            },
            timeout=10,
        )
    except requests.RequestException as e:
        log.warning(f"  Erro de rede em {categoria['nome']}: {e}")
        return None

    if resp.status_code != 200:
        log.warning(f"  NewsAPI HTTP {resp.status_code} em {categoria['nome']}: {resp.text[:200]}")
        return None

    articles = resp.json().get("articles", [])

    for a in articles:
        titulo    = (a.get("title") or "").strip()
        descricao = (a.get("description") or "").strip()
        url       = (a.get("url") or "").strip()

        # Filtra artigos sem conteúdo útil ou duplicados
        if not titulo or titulo == "[Removed]":
            continue
        if not descricao or descricao == "[Removed]":
            continue
        if url in urls_vistos:
            continue

        urls_vistos.add(url)
        return {
            "categoria": categoria["nome"],
            "slug":      categoria["slug"],
            "titulo":    titulo,
            "descricao": descricao,
            "url":       url,
            "fonte":     (a.get("source") or {}).get("name", ""),
            "data_pub":  a.get("publishedAt", ""),
        }

    log.warning(f"  Nenhum artigo válido encontrado para {categoria['nome']}")
    return None


# ── Firestore ─────────────────────────────────────────────────────────────────

def salvar_firestore(fs: firestore.Client, periodo: str, itens: list[dict]):
    fs.collection("noticias").document(periodo).set({
        "periodo":    periodo,
        "gerado_em":  datetime.now(timezone.utc).isoformat(),
        "itens":      itens,
    })
    log.info(f"Firestore → noticias/{periodo} ({len(itens)} notícia(s))")


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== Job Notícias iniciado ===")

    # Determina o período pelo horário de Brasília (UTC-3)
    agora_brt = datetime.now(timezone(timedelta(hours=-3)))
    periodo   = "manha" if agora_brt.hour < 12 else "tarde"
    log.info(f"Horário BRT: {agora_brt.strftime('%H:%M')} → período: {periodo}")

    fs_client   = firestore.Client(project=FS_PROJECT)
    urls_vistos: set = set()
    itens: list[dict] = []

    for cat in CATEGORIAS:
        if len(itens) >= MAX_NOTICIAS:
            break

        log.info(f"Buscando: {cat['nome']}...")
        noticia = buscar_noticia(cat, urls_vistos)

        if noticia:
            itens.append(noticia)
            log.info(f"  ✓ {noticia['titulo'][:80]}")

        # Respeita rate limit da NewsAPI (1 req/s no plano gratuito)
        time.sleep(1)

    if not itens:
        log.error("Nenhuma notícia encontrada. Encerrando sem salvar.")
        raise SystemExit(1)

    salvar_firestore(fs_client, periodo, itens)
    log.info(f"=== Job concluído — {len(itens)} notícia(s) salva(s) em '{periodo}' ===")
