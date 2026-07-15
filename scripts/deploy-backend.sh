#!/usr/bin/env bash
# Deploy do backend (api-creta) para um cliente ou para todos.
#
# Uso:
#   ./scripts/deploy-backend.sh creta      # um cliente específico
#   ./scripts/deploy-backend.sh all        # todos os clientes em clients/*.env
#
# Cada cliente precisa ter os secrets newsapi-key, btg-client-id e
# btg-client-secret já criados no Secret Manager do próprio projeto GCP dele.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
CLIENTS_DIR="$REPO_ROOT/clients"

deploy_one() {
  local env_file="$1"
  # shellcheck disable=SC1090
  source "$env_file"

  echo "── Deploy api-creta — cliente: ${CLIENT_SLUG} (projeto ${GCP_PROJECT}) ──"

  gcloud run deploy api-creta \
    --source "$REPO_ROOT/api-creta" \
    --region "$REGION" \
    --project "$GCP_PROJECT" \
    --set-env-vars "^@^GCP_PROJECT=${GCP_PROJECT}@BQ_DATASET=${BQ_DATASET}@GCS_BUCKET=${GCS_BUCKET}@FIREBASE_PROJECT=${FIREBASE_PROJECT}@ALLOWED_ORIGINS=${ALLOWED_ORIGINS}" \
    --set-secrets NEWSAPI_KEY=newsapi-key:latest,BTG_CLIENT_ID=btg-client-id:latest,BTG_CLIENT_SECRET=btg-client-secret:latest \
    --quiet

  echo "── OK: ${CLIENT_SLUG} ──"
}

target="${1:-}"
if [ -z "$target" ]; then
  echo "Uso: $0 <slug-do-cliente|all>" >&2
  exit 1
fi

if [ "$target" = "all" ]; then
  for env_file in "$CLIENTS_DIR"/*.env; do
    deploy_one "$env_file"
  done
else
  env_file="$CLIENTS_DIR/${target}.env"
  if [ ! -f "$env_file" ]; then
    echo "Cliente '${target}' não encontrado em ${CLIENTS_DIR}/" >&2
    exit 1
  fi
  deploy_one "$env_file"
fi
