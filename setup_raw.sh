#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Bootstrap em 1 Comando — Camada Raw Open Lakehouse (BigLake Apache Iceberg)
# Uso:
#   ./setup_raw.sh                     # Detecta PROJECT_ID via config.json ou gcloud
#   ./setup_raw.sh meu-projeto-gcp     # Informa PROJECT_ID explicitamente
#   ./setup_raw.sh --cli               # Força provisionamento direto via API/CLI (sem Terraform)
#   ./setup_raw.sh --destroy           # Remove recursos via Terraform (se provisionado por TF)
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="${REGION:-us-central1}"
FORCE_CLI="false"
DESTROY_MODE="false"
ARG_PROJECT_ID=""

for arg in "$@"; do
  case "$arg" in
    --cli)
      FORCE_CLI="true"
      ;;
    --destroy)
      DESTROY_MODE="true"
      ;;
    --help|-h)
      echo "Uso: ./setup_raw.sh [PROJECT_ID] [--cli] [--destroy]"
      exit 0
      ;;
    -*)
      echo "Opção desconhecida: $arg" >&2
      exit 1
      ;;
    *)
      ARG_PROJECT_ID="$arg"
      ;;
  esac
done

export CLOUDSDK_METRICS_ENVIRONMENT="${CLOUDSDK_METRICS_ENVIRONMENT:+$CLOUDSDK_METRICS_ENVIRONMENT }datacloud.antigravity"

# Resolver PROJECT_ID (1. Argumento CLI -> 2. config.json local/pai -> 3. gcloud config)
PROJECT_ID="${ARG_PROJECT_ID}"
if [[ -z "${PROJECT_ID}" ]]; then
  for cfg in "${SCRIPT_DIR}/config.json" "${SCRIPT_DIR}/../config.json"; do
    if [[ -f "${cfg}" ]]; then
      PROJECT_ID="$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('PROJECT_ID') or d.get('project_id') or '')" "${cfg}" 2>/dev/null || true)"
      [[ -n "${PROJECT_ID}" ]] && break
    fi
  done
fi

if [[ -z "${PROJECT_ID}" ]]; then
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
  [[ "${PROJECT_ID}" == "(unset)" ]] && PROJECT_ID=""
fi

if [[ -z "${PROJECT_ID}" ]]; then
  echo "ERRO: Nenhum PROJECT_ID detectado. Passe './setup_raw.sh SEU_PROJECT_ID' ou configure 'gcloud config set project SEU_PROJECT_ID'." >&2
  exit 1
fi

echo "======================================================================"
echo " Retail Open Lakehouse — Setup Camada Raw (BigLake Apache Iceberg)"
echo " Project ID : ${PROJECT_ID}"
echo " Região     : ${REGION}"
echo "======================================================================"

if command -v terraform >/dev/null 2>&1 && [[ "${FORCE_CLI}" != "true" ]]; then
  echo "-> Terraform detectado ($(terraform version -json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('terraform_version',''))" 2>/dev/null || echo 'ativo')). Executando via Terraform..."

  # Garantir ADC para o provider hashicorp/google
  if ! gcloud auth application-default print-access-token >/dev/null 2>&1; then
    echo "-> Credenciais ADC não encontradas. Abrindo login no navegador..."
    gcloud auth application-default login
  fi

  terraform -chdir="${SCRIPT_DIR}/terraform" init -input=false

  if [[ "${DESTROY_MODE}" == "true" ]]; then
    terraform -chdir="${SCRIPT_DIR}/terraform" destroy -auto-approve -var="project_id=${PROJECT_ID}" -var="region=${REGION}"
    exit 0
  fi

  # Importar automaticamente bucket/dataset/connection se já existirem no projeto de laboratório
  BUCKET_NAME="${PROJECT_ID}-retail-lakehouse"
  if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    terraform -chdir="${SCRIPT_DIR}/terraform" import -var="project_id=${PROJECT_ID}" -var="region=${REGION}" google_storage_bucket.lakehouse "${BUCKET_NAME}" >/dev/null 2>&1 || true
  fi
  if bq show --project_id="${PROJECT_ID}" "retail_raw" >/dev/null 2>&1; then
    terraform -chdir="${SCRIPT_DIR}/terraform" import -var="project_id=${PROJECT_ID}" -var="region=${REGION}" google_bigquery_dataset.retail_raw "projects/${PROJECT_ID}/datasets/retail_raw" >/dev/null 2>&1 || true
  fi
  if bq show --connection --project_id="${PROJECT_ID}" --location="${REGION}" "retail_iceberg_conn" >/dev/null 2>&1; then
    terraform -chdir="${SCRIPT_DIR}/terraform" import -var="project_id=${PROJECT_ID}" -var="region=${REGION}" google_bigquery_connection.iceberg_conn "projects/${PROJECT_ID}/locations/${REGION}/connections/retail_iceberg_conn" >/dev/null 2>&1 || true
  fi

  terraform -chdir="${SCRIPT_DIR}/terraform" apply -auto-approve -var="project_id=${PROJECT_ID}" -var="region=${REGION}"
else
  if [[ "${DESTROY_MODE}" == "true" ]]; then
    echo "ERRO: '--destroy' requer o binário terraform instalado." >&2
    exit 1
  fi
  echo "-> Executando provisionamento paralelo direto via Google Cloud APIs/CLI..."
  python3 "${SCRIPT_DIR}/scripts/provision_raw_fallback.py" --project-id "${PROJECT_ID}" --region "${REGION}"
fi
