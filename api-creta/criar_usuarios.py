#!/usr/bin/env python3
"""
criar_usuarios.py — Cria usuários no Firebase Auth e perfis no Firestore.

Como usar:
  1. Ative o venv e entre na pasta api-creta:
       source venv/bin/activate
  2. Execute:
       python criar_usuarios.py

O script imprime as senhas temporárias no terminal. Guarde-as para enviar
a cada assessor. Eles deverão alterar a senha no primeiro acesso.

IMPORTANTE: Não compartilhe as senhas por canais inseguros.
Execute apenas uma vez. Se precisar rodar novamente, comente os usuários
que já existem ou use a flag --update.
"""

import secrets
import string
import sys
import firebase_admin
from firebase_admin import credentials, auth, firestore

# ── Inicialização (usa Application Default Credentials via gcloud) ────────────
if not firebase_admin._apps:
    firebase_admin.initialize_app(options={"projectId": "creta-btg-bd3a8"})

db = firestore.client()


# ── Mapeamento de usuários ─────────────────────────────────────────────────────
# assessor_name deve bater EXATAMENTE com o campo Assessor_Manual no BigQuery.
# role: "admin" → acessa todos os dados | "assessor" → vê só os próprios dados

USUARIOS = [
    {
        "email":          "gustavo.farias@cretainvestimentos.com.br",
        "displayName":    "Gustavo Ribeiro De Farias",
        "role":           "assessor",
        "assessor_name":  "Gustavo Ribeiro De Farias",
    },
    {
        "email":          "Oscar.monteiro@cretainvestimentos.com.br",
        "displayName":    "Oscar Viassa Lehel Monteiro",
        "role":           "assessor",
        "assessor_name":  "Oscar Viassa Lehel Monteiro",
    },
    {
        "email":          "clarissa.dutra@cretacapital.com.br",
        "displayName":    "Clarissa Maria Castro Aguiar Dutra",
        "role":           "assessor",
        "assessor_name":  "Clarissa Maria Castro Aguiar Dutra",
    },
    {
        "email":          "rodrigo.chiote@cretainvestimentos.com.br",
        "displayName":    "Rodrigo De Oliveira Chiote Pinheiro",
        "role":           "assessor",
        "assessor_name":  "Rodrigo De Oliveira Chiote Pinheiro",
    },
    {
        "email":          "aline.rodrigues@cretainvestimentos.com.br",
        "displayName":    "Aline Rodrigues",
        "role":           "assessor",
        "assessor_name":  "Aline Rodrigues",
    },
    {
        "email":          "fillipe.sousa@cretainvestimentos.com.br",
        "displayName":    "Fillipe Maciel Sousa",
        "role":           "assessor",
        "assessor_name":  "Fillipe Maciel Sousa",
    },
    {
        "email":          "manu.lombardi@cretainvestimentos.com.br",
        "displayName":    "Emmanoelle Isabel Lombardi",
        "role":           "admin",
        "assessor_name":  None,  # admin vê todos os dados
    },
]

# Admins que já podem existir (conta pessoal Fillipe)
# Serão criados no Firestore mesmo que o Auth já exista.
ADMINS_EXISTENTES = [
    {
        "email":          "fillipemsousa@gmail.com",
        "displayName":    "Fillipe Maciel Sousa (admin)",
        "role":           "admin",
        "assessor_name":  None,
    },
]


def senha_aleatoria(n: int = 12) -> str:
    """Gera senha com letras, dígitos e símbolos básicos."""
    alfa = string.ascii_letters + string.digits + "!@#$"
    while True:
        pwd = "".join(secrets.choice(alfa) for _ in range(n))
        # Garante ao menos 1 maiúscula, 1 minúscula, 1 dígito
        if (any(c.isupper() for c in pwd) and
                any(c.islower() for c in pwd) and
                any(c.isdigit() for c in pwd)):
            return pwd


def salvar_firestore(uid: str, dados: dict):
    """Grava/atualiza o perfil do usuário no Firestore."""
    doc = {
        "displayName":       dados["displayName"],
        "email":             dados["email"],
        "role":              dados["role"],
        "assessor_name":     dados.get("assessor_name"),
        "mustChangePassword": True,
        "createdAt":         firestore.SERVER_TIMESTAMP,
    }
    db.collection("users").document(uid).set(doc, merge=True)

    # Custom claims no JWT — permite que a API valide role/assessor sem ler Firestore
    claims = {"role": dados["role"]}
    if dados.get("assessor_name"):
        claims["assessor_name"] = dados["assessor_name"]
    auth.set_custom_user_claims(uid, claims)
    print(f"             Claims definidos: {claims}")


def criar_ou_atualizar(usuario: dict) -> tuple[str, str, str]:
    """
    Tenta criar o usuário no Firebase Auth.
    Se já existir, atualiza apenas o Firestore (não redefine a senha).
    Retorna (email, senha, status).
    """
    email = usuario["email"]
    pwd   = senha_aleatoria()

    try:
        user = auth.create_user(
            email=email,
            password=pwd,
            display_name=usuario["displayName"],
            email_verified=False,
        )
        salvar_firestore(user.uid, usuario)
        return (email, pwd, "✅ criado")

    except auth.EmailAlreadyExistsError:
        user = auth.get_user_by_email(email)
        salvar_firestore(user.uid, usuario)
        return (email, "— (já existia, senha mantida)", "⚠️  Firestore atualizado")

    except Exception as exc:
        return (email, "—", f"❌ ERRO: {exc}")


def main():
    print("\n" + "=" * 65)
    print("  Portal Creta — Criação de Usuários Firebase")
    print("=" * 65 + "\n")

    todos = USUARIOS + ADMINS_EXISTENTES
    resultados = []

    for u in todos:
        email, pwd, status = criar_ou_atualizar(u)
        resultados.append((email, pwd, status, u["role"]))
        print(f"  {status}  [{u['role'].upper()}]  {email}")
        if "criado" in status:
            print(f"             Senha temporária: {pwd}")

    # Sumário final
    print("\n" + "=" * 65)
    print("  SENHAS TEMPORÁRIAS — envie a cada usuário de forma segura")
    print("=" * 65)
    print(f"  {'EMAIL':<45} {'SENHA':<14} {'PAPEL'}")
    print("  " + "-" * 62)
    for email, pwd, status, role in resultados:
        print(f"  {email:<45} {pwd:<14} {role}")
    print("\n  ⚠️  Oriente cada usuário a alterar a senha após o primeiro login.")
    print("     Link de acesso: abra o portal e clique em 'Esqueci minha senha'\n")


if __name__ == "__main__":
    main()
