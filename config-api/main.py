"""
config-api — Cloud Run service (projeto synciadesk-hosting)

Serve a config de Firebase/API de cada cliente por subdomínio, e expõe um
CRUD admin pra cadastrar clientes novos sem precisar editar código.

Endpoints:
  GET  /config.js       → público. Lê o host original (X-Forwarded-Host,
                           preservado pelo Firebase Hosting) e devolve
                           `window.APP_CONFIG = {...}` em JS.
  GET  /api/clients      → admin only. Lista clientes cadastrados.
  POST /api/clients      → admin only. Cria/atualiza um cliente.
  PUT  /api/clients/{h}  → admin only. Atualiza um cliente.
  DEL  /api/clients/{h}  → admin only. Remove um cliente.

O login do admin usa o Firebase Auth do projeto Creta (creta-btg-bd3a8) —
os mesmos e-mails que já são admin lá. Os dados dos clientes ficam no
Firestore deste projeto (synciadesk-hosting), sem relação com os dados de
nenhum cliente específico.
"""

import json
import logging
import os
from typing import Optional

import firebase_admin
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import Response
from firebase_admin import auth as firebase_auth
from firebase_admin import firestore as fb_firestore
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("config-api")

# Configurado via env var (Secret Manager: secret "admin-emails"), lista separada por vírgula.
ADMIN_EMAILS = {e.strip() for e in os.getenv("ADMIN_EMAILS", "").split(",") if e.strip()}

# Projeto cujo Firebase Auth autentica o login do admin.
AUTH_PROJECT = os.getenv("AUTH_PROJECT", "creta-btg-bd3a8")
# Projeto deste serviço — onde fica o Firestore com o registro de clientes.
GCP_PROJECT = os.getenv("GCP_PROJECT", "synciadesk-hosting")
# Cliente usado como fallback quando o host não está cadastrado.
DEFAULT_HOST = os.getenv("DEFAULT_HOST", "creta.synciadesk.com.br")

# App padrão: Firestore deste projeto.
if not firebase_admin._apps:
    firebase_admin.initialize_app(options={"projectId": GCP_PROJECT})
db = fb_firestore.client()

# App nomeado à parte, só pra validar tokens emitidos pelo Auth da Creta.
_AUTH_APP_NAME = "admin-auth"
try:
    auth_app = firebase_admin.get_app(_AUTH_APP_NAME)
except ValueError:
    auth_app = firebase_admin.initialize_app(
        options={"projectId": AUTH_PROJECT}, name=_AUTH_APP_NAME
    )

app = FastAPI(title="config-api", version="1.0.0")


def verificar_admin(authorization: Optional[str] = Header(None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token de autenticação ausente.")
    token = authorization.split(" ", 1)[1]
    try:
        decoded = firebase_auth.verify_id_token(token, app=auth_app)
    except Exception as e:
        log.warning(f"Token inválido: {e}")
        raise HTTPException(status_code=401, detail="Token expirado ou inválido.")

    email = decoded.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Acesso restrito a administradores.")
    return email


class ClienteIn(BaseModel):
    apiUrl: str
    firebaseConfig: dict


def _client_doc_to_response(hostname: str, data: dict) -> dict:
    return {
        "hostname": hostname,
        "apiUrl": data.get("apiUrl", ""),
        "firebaseConfig": data.get("firebaseConfig", {}),
    }


@app.get("/config.js")
async def config_js(request: Request):
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or DEFAULT_HOST
    host = host.split(":")[0]

    doc = db.collection("clients").document(host).get()
    if not doc.exists:
        doc = db.collection("clients").document(DEFAULT_HOST).get()

    data = doc.to_dict() if doc.exists else {"firebaseConfig": {}, "apiUrl": ""}
    payload = {
        "firebaseConfig": data.get("firebaseConfig", {}),
        "apiUrl": data.get("apiUrl", ""),
    }
    body = f"window.APP_CONFIG = {json.dumps(payload)};"
    return Response(
        content=body,
        media_type="application/javascript",
        headers={"Cache-Control": "public, max-age=300"},
    )


@app.get("/api/clients")
async def listar_clientes(email: str = Depends(verificar_admin)):
    docs = db.collection("clients").stream()
    return [_client_doc_to_response(d.id, d.to_dict()) for d in docs]


@app.post("/api/clients/{hostname}")
async def salvar_cliente(hostname: str, body: ClienteIn, email: str = Depends(verificar_admin)):
    db.collection("clients").document(hostname).set(
        {"apiUrl": body.apiUrl, "firebaseConfig": body.firebaseConfig}
    )
    return _client_doc_to_response(hostname, body.dict())


@app.delete("/api/clients/{hostname}")
async def apagar_cliente(hostname: str, email: str = Depends(verificar_admin)):
    if hostname == DEFAULT_HOST:
        raise HTTPException(status_code=400, detail="Não é possível apagar o cliente padrão.")
    db.collection("clients").document(hostname).delete()
    return {"ok": True}


@app.get("/health")
async def health():
    return {"status": "ok"}
