#!/usr/bin/env bash
# Provisiona a infraestrutura de um cliente novo (GCP + Firebase + BigQuery +
# Cloud Run). Roda direto no terminal — usa a autenticação já existente do
# gcloud/firebase local (não pede login).
#
# Uso:
#   ./scripts/provisionar-cliente.sh interno
#   ./scripts/provisionar-cliente.sh ewz

set -euo pipefail

CLIENT_SLUG="${1:?Uso: $0 <slug-do-cliente>}"
GCP_PROJECT="${CLIENT_SLUG}-portal"
FIREBASE_PROJECT="$GCP_PROJECT"
REGION="us-central1"
BQ_DATASET="dados_crus"
GCS_BUCKET="${GCP_PROJECT}-pipeline"
DOMAIN="${CLIENT_SLUG}.synciadesk.com.br"
BILLING_ACCOUNT="01F53C-019FE6-2C4878"

REF_PROJECT="creta-btg"
REF_DATASET="dados_crus"
TABELAS=(
  receitas_para_repasse posicao_das_contas suitability_contas
  conta_assessor_excecoes conta_assessor_base webhook_btg_raw
  carteira_recomendada_allocation carteira_recomendada_portfolio
  conta_primeira_aparicao partner_report_cdb_lca_lci_lf
)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "══════════════════════════════════════════════════"
echo " Provisionando: $CLIENT_SLUG → projeto $GCP_PROJECT → domínio $DOMAIN"
echo "══════════════════════════════════════════════════"

echo "── 1. Criando projeto GCP e vinculando faturamento ──"
if gcloud projects describe "$GCP_PROJECT" >/dev/null 2>&1; then
  echo "Projeto $GCP_PROJECT já existe — pulando criação."
else
  gcloud projects create "$GCP_PROJECT" --name="$CLIENT_SLUG"
fi
gcloud billing projects link "$GCP_PROJECT" --billing-account="$BILLING_ACCOUNT"

echo "── 2. Ativando APIs ──"
gcloud services enable \
  firestore.googleapis.com identitytoolkit.googleapis.com \
  run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  bigquery.googleapis.com storage.googleapis.com secretmanager.googleapis.com \
  cloudscheduler.googleapis.com \
  --project="$GCP_PROJECT"

echo "── 3. Permissões (Cloud Build + acionar Cloud Run Jobs via Scheduler) ──"
PROJECT_NUMBER="$(gcloud projects describe "$GCP_PROJECT" --format='value(projectNumber)')"
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member="serviceAccount:${COMPUTE_SA}" \
  --role="roles/cloudbuild.builds.builder" >/dev/null
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member="serviceAccount:${COMPUTE_SA}" \
  --role="roles/run.developer" >/dev/null
echo "Service account: $COMPUTE_SA"

echo "── 4. Projeto Firebase + Firestore ──"
firebase projects:addfirebase "$GCP_PROJECT" || echo "(Firebase já estava ativo nesse projeto — ok)"
if gcloud firestore databases describe --project="$GCP_PROJECT" >/dev/null 2>&1; then
  echo "Firestore já existe — pulando criação."
else
  gcloud firestore databases create --project="$GCP_PROJECT" --location="$REGION" --type=firestore-native
fi

echo "── 5. Bucket GCS ──"
if gcloud storage buckets describe "gs://${GCS_BUCKET}" >/dev/null 2>&1; then
  echo "Bucket já existe — pulando criação."
else
  gcloud storage buckets create "gs://${GCS_BUCKET}" --project="$GCP_PROJECT" --location="$REGION"
fi

echo "── 6. Dataset BigQuery + schema das tabelas (sem dados) ──"
if bq show --dataset "${GCP_PROJECT}:${BQ_DATASET}" >/dev/null 2>&1; then
  echo "Dataset já existe — pulando criação."
else
  bq mk --dataset --project_id="$GCP_PROJECT" --location="$REGION" "${GCP_PROJECT}:${BQ_DATASET}"
fi

for tabela in "${TABELAS[@]}"; do
  if bq show "${GCP_PROJECT}:${BQ_DATASET}.${tabela}" >/dev/null 2>&1; then
    echo "✓ tabela já existe: $tabela (pulei)"
    continue
  fi
  schema_path="/tmp/${tabela}_schema.json"
  if bq show --schema --format=json "${REF_PROJECT}:${REF_DATASET}.${tabela}" > "$schema_path" 2>/dev/null; then
    bq mk --table --project_id="$GCP_PROJECT" "${GCP_PROJECT}:${BQ_DATASET}.${tabela}" "$schema_path"
    echo "✓ tabela criada: $tabela"
  else
    echo "⚠ não encontrei $tabela em ${REF_PROJECT}.${REF_DATASET} — pulei."
  fi
done

echo "── 7. Secrets no Secret Manager ──"
read -rsp "BTG_CLIENT_ID do cliente (Enter pra pular): " BTG_CLIENT_ID; echo
read -rsp "BTG_CLIENT_SECRET do cliente (Enter pra pular): " BTG_CLIENT_SECRET; echo
read -rsp "E-mails de admin, separados por vírgula (obrigatório): " ADMIN_EMAILS_VALUE; echo

salvar_secret() {
  local nome="$1"
  local valor="$2"
  if [ -z "$valor" ]; then
    echo "⚠ pulei $nome (vazio) — cria depois manualmente se precisar."
    return
  fi
  printf '%s' "$valor" | gcloud secrets create "$nome" --project="$GCP_PROJECT" --data-file=- 2>/dev/null || \
  printf '%s' "$valor" | gcloud secrets versions add "$nome" --project="$GCP_PROJECT" --data-file=-
  gcloud secrets add-iam-policy-binding "$nome" \
    --project="$GCP_PROJECT" \
    --member="serviceAccount:${COMPUTE_SA}" \
    --role="roles/secretmanager.secretAccessor" >/dev/null
  echo "✓ secret salvo: $nome"
}

salvar_secret "btg-client-id" "$BTG_CLIENT_ID"
salvar_secret "btg-client-secret" "$BTG_CLIENT_SECRET"
salvar_secret "admin-emails" "$ADMIN_EMAILS_VALUE"

echo "── 8. Deploy do backend (api-creta) ──"
ALLOWED_ORIGINS="https://${DOMAIN},http://localhost:8080,http://127.0.0.1:8080"

gcloud run deploy api-creta \
  --source "${REPO_ROOT}/api-creta" \
  --region "$REGION" \
  --project "$GCP_PROJECT" \
  --set-env-vars "^@^GCP_PROJECT=${GCP_PROJECT}@BQ_DATASET=${BQ_DATASET}@GCS_BUCKET=${GCS_BUCKET}@FIREBASE_PROJECT=${FIREBASE_PROJECT}@ALLOWED_ORIGINS=${ALLOWED_ORIGINS}" \
  --set-secrets BTG_CLIENT_ID=btg-client-id:latest,BTG_CLIENT_SECRET=btg-client-secret:latest,ADMIN_EMAILS=admin-emails:latest \
  --allow-unauthenticated \
  --quiet

API_URL="$(gcloud run services describe api-creta --project="$GCP_PROJECT" --region="$REGION" --format='value(status.url)')"

echo "── 9. Deploy dos jobs automatizados (posições, suitability, carteira recomendada) ──"
for job in job-posicoes job-suitability job-carteira-recomendada; do
  gcloud run jobs deploy "$job" \
    --source "${REPO_ROOT}/${job}" \
    --region "$REGION" \
    --project "$GCP_PROJECT" \
    --set-env-vars "GCP_PROJECT=${GCP_PROJECT},BQ_DATASET=${BQ_DATASET}" \
    --set-secrets "BTG_CLIENT_ID=btg-client-id:latest,BTG_CLIENT_SECRET=btg-client-secret:latest" \
    --quiet
done

echo "── 10. Agendamento diário (Cloud Scheduler) ──"
criar_scheduler() {
  local nome="$1"
  local job="$2"
  local cron="$3"
  gcloud scheduler jobs create http "$nome" \
    --project="$GCP_PROJECT" \
    --location="$REGION" \
    --schedule="$cron" \
    --time-zone="America/Sao_Paulo" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${GCP_PROJECT}/jobs/${job}:run" \
    --http-method=POST \
    --oauth-service-account-email="$COMPUTE_SA" \
    --quiet 2>/dev/null || echo "  ($nome já existe — pulei)"
}
criar_scheduler "scheduler-posicoes" "job-posicoes" "0 8 * * 1-5"
criar_scheduler "scheduler-suitability" "job-suitability" "0 8 * * 1"
criar_scheduler "scheduler-carteira-recomendada" "job-carteira-recomendada" "0 8 * * 1"

echo "── 11. Config web do Firebase ──"
if ! firebase apps:sdkconfig web --project "$GCP_PROJECT" 2>/tmp/sdkconfig_err; then
  echo "Ainda não existe um app Web nesse projeto Firebase. Rodando 'firebase apps:create web'..."
  firebase apps:create web "$CLIENT_SLUG" --project "$GCP_PROJECT"
  firebase apps:sdkconfig web --project "$GCP_PROJECT"
fi

cat <<EOF

══════════════════════════════════════════════════
RESUMO — $CLIENT_SLUG
──────────────────────────────────────────────────
Projeto GCP/Firebase: $GCP_PROJECT
API:                  $API_URL
Domínio alvo:         $DOMAIN

Falta fazer, manualmente:
1. Ir em admin.synciadesk.com.br → "Novo cliente" → preencher:
   - Subdomínio: $DOMAIN
   - URL da API: $API_URL
   - Firebase config: usar a saída da etapa 11 acima
2. No Firebase Console do projeto $GCP_PROJECT → Authentication →
   Sign-in method → ativar Email/Senha.
3. No Firebase Console do projeto $GCP_PROJECT → Authentication →
   Settings → Authorized domains → adicionar: $DOMAIN
4. No Firebase Console do projeto synciadesk-hosting → Hosting →
   Adicionar domínio customizado → $DOMAIN → pegar o registro DNS
   e cadastrar no registro.br.
5. Rodar api-creta/criar_usuarios.py (apontando pro projeto $GCP_PROJECT)
   pra criar os primeiros usuários do escritório.
══════════════════════════════════════════════════
EOF
