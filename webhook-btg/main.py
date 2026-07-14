"""
webhook-btg — Cloud Run service (us-east1)
Recebe webhooks do BTG Pactual, baixa o arquivo S3 imediatamente
e salva no GCS antes que a URL assinada expire (TTL = 1h).

Tipos tratados:
  account-advisor       → gs://creta-btg-pipeline/entradas/
  partner-report        → gs://creta-btg-pipeline/carteira-recomendada/
  operation-history     → Firestore movimentacoes/{conta}
  (outros desconhecidos) → gs://creta-btg-pipeline/webhooks-raw/
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from google.cloud import bigquery, storage as gcs, firestore as gcp_firestore

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("webhook-btg")

# ── Configuração ──────────────────────────────────────────────────────────────
GCP_PROJECT    = os.getenv("GCP_PROJECT",    "creta-btg")
DATASET        = os.getenv("BQ_DATASET",     "dados_crus")
GCS_BUCKET     = os.getenv("GCS_BUCKET",     "creta-btg-pipeline")
WEBHOOK_TOKEN  = os.getenv("WEBHOOK_TOKEN",  "creta-btg-webhook-2024")
TABLE_WEBHOOK  = f"{GCP_PROJECT}.{DATASET}.webhook_btg_raw"

# ── Clientes GCP ──────────────────────────────────────────────────────────────
FS_PROJECT  = os.getenv("FS_PROJECT", "creta-btg-bd3a8")
bq_client   = bigquery.Client(project=GCP_PROJECT)
gcs_client  = gcs.Client(project=GCP_PROJECT)
fs_client   = gcp_firestore.Client(project=FS_PROJECT)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Webhook BTG", version="2.0.0")


def _gcs_folder(s3_url: str) -> str:
    """Determina pasta GCS pelo tipo de arquivo na URL S3."""
    if "account-advisor" in s3_url:
        return "entradas/"
    if "partner-report" in s3_url:
        return "carteira-recomendada/"
    return "webhooks-raw/"


def _salvar_movimentacoes(payload: dict, received_at: str) -> str:
    """
    Salva dados de movimentação no Firestore.
    Retorna o nome do documento salvo (= número da conta).
    """
    # Tenta extrair número da conta — BTG pode enviar como accountNumber, account, conta etc.
    conta = (
        str(payload.get("accountNumber") or
            payload.get("account") or
            payload.get("conta") or
            (payload.get("response") or {}).get("accountNumber") or
            "desconhecida")
    )

    # Dados de operações — tenta campos comuns
    dados = (
        payload.get("operations") or
        payload.get("data") or
        payload.get("items") or
        (payload.get("response") or {}).get("operations") or
        []
    )

    fs_client.collection("movimentacoes").document(conta).set({
        "conta":         conta,
        "status":        "disponivel",
        "dados":         dados,
        "payload_bruto": json.dumps(payload, ensure_ascii=False)[:65000],  # limite Firestore
        "atualizado_em": received_at,
        "solicitado_em": gcp_firestore.SERVER_TIMESTAMP,  # mantém se já existia
    }, merge=True)

    # Corrige o campo solicitado_em para não sobrescrever com SERVER_TIMESTAMP se já existe
    fs_client.collection("movimentacoes").document(conta).update({
        "status":        "disponivel",
        "dados":         dados,
        "atualizado_em": received_at,
    })

    log.info(f"Movimentações salvas no Firestore: conta={conta} operações={len(dados)}")
    return conta


def _e_payload_movimentacao(payload: dict) -> bool:
    """
    Detecta se o payload é de operation-history (movimentações).
    Critérios: presença de campos típicos de movimentação sem URL S3.
    """
    # Se tiver URL S3 → é arquivo (não movimentação)
    resp = payload.get("response") or {}
    if payload.get("url") or resp.get("url"):
        s3_url = payload.get("url") or resp.get("url") or ""
        if "operation-history" not in s3_url:
            return False

    # Campos típicos de movimentação
    tem_operacoes = any(k in payload for k in ("operations", "items", "data"))
    tem_conta     = any(k in payload for k in ("accountNumber", "account", "conta"))
    tem_evento    = "operation" in str(payload.get("eventType", "")).lower()

    return tem_operacoes or (tem_conta and not (payload.get("url") or resp.get("url")))


def _gravar_bigquery(payload: dict, received_at: str) -> None:
    """Grava payload bruto na tabela webhook_btg_raw."""
    try:
        erros = bq_client.insert_rows_json(
            TABLE_WEBHOOK,
            [{"payload": json.dumps(payload, ensure_ascii=False), "received_at": received_at}],
        )
        if erros:
            log.error(f"BigQuery insert errors: {erros}")
    except Exception as e:
        log.error(f"Erro ao gravar BigQuery: {e}")


def _baixar_e_salvar(s3_url: str) -> str:
    """Baixa arquivo S3 e salva no GCS. Retorna o caminho gs:// do arquivo."""
    folder   = _gcs_folder(s3_url)
    filename = s3_url.split("?")[0].split("/")[-1]
    gcs_path = folder + filename

    log.info(f"Baixando: {filename}")
    r = requests.get(s3_url, timeout=30)
    r.raise_for_status()

    bucket = gcs_client.bucket(GCS_BUCKET)
    blob   = bucket.blob(gcs_path)
    blob.upload_from_string(r.content, content_type="application/octet-stream")

    destino = f"gs://{GCS_BUCKET}/{gcs_path}"
    log.info(f"Salvo em {destino} ({len(r.content):,} bytes)")
    return destino


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.post("/")
@app.post("/webhook")
async def receber_webhook(request: Request, token: Optional[str] = Query(default=None)):
    """
    Endpoint principal — aceita POST em / e /webhook.
    O BTG envia: https://webhook-btg-....run.app?token=creta-btg-webhook-2024
    """
    # ── Valida token ──────────────────────────────────────────────────────────
    if token != WEBHOOK_TOKEN:
        log.warning(f"Token inválido recebido: {token!r}")
        raise HTTPException(status_code=403, detail="Token inválido.")

    # ── Lê payload ────────────────────────────────────────────────────────────
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Payload JSON inválido.")

    received_at = datetime.utcnow().isoformat() + "Z"
    log.info(f"Webhook recebido: keys={list(payload.keys())}")

    # ── Grava no BigQuery ─────────────────────────────────────────────────────
    _gravar_bigquery(payload, received_at)

    # ── Verifica erros no payload ─────────────────────────────────────────────
    erros_payload = payload.get("errors") or []
    if erros_payload:
        log.warning(f"Payload com erros BTG: {erros_payload}")
        return {"ok": True, "arquivo": None, "aviso": "Payload com erros — sem arquivo."}

    # ── Detecta se é payload de movimentações (operation-history) ────────────
    if _e_payload_movimentacao(payload):
        log.info("Payload identificado como movimentação (operation-history)")
        try:
            conta = _salvar_movimentacoes(payload, received_at)
        except Exception as e:
            log.error(f"Falha ao salvar movimentações no Firestore: {e}")
            return {"ok": True, "conta": None, "aviso": str(e)}
        return {"ok": True, "tipo": "movimentacao", "conta": conta, "received_at": received_at}

    # ── Extrai URL S3 (arquivos de posição / carteira recomendada) ────────────
    # Formato A (account-advisor):  {"url": "...", "fileSize": ..., ...}
    # Formato B (partner-report):   {"errors": [], "response": {"url": "...", ...}}
    resp   = payload.get("response") or {}
    s3_url = payload.get("url") or resp.get("url")

    if not s3_url:
        log.info("Sem URL S3 no payload — nada a baixar.")
        return {"ok": True, "arquivo": None}

    # ── Baixa e salva no GCS ──────────────────────────────────────────────────
    try:
        destino = _baixar_e_salvar(s3_url)
    except Exception as e:
        # Retorna 200 para o BTG não retentar — o payload já está no BigQuery
        log.error(f"Falha ao baixar/salvar arquivo: {e}")
        return {"ok": True, "arquivo": None, "aviso": str(e)}

    return {"ok": True, "arquivo": destino, "received_at": received_at}
