#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Bootstrap Ultra-Rápido (~40s) — Camada Raw Open Lakehouse + Setup 2 Trilhas
# Uso:
#   ./setup_raw.sh                     # Execução paralela rápida (~40s via Python/REST API)
#   ./setup_raw.sh meu-projeto-gcp     # Informa PROJECT_ID explicitamente
#   ./setup_raw.sh --terraform         # Opcional: provisiona via módulo declarativo Terraform
#   ./setup_raw.sh --destroy           # Remove recursos via Terraform
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="${REGION:-us-central1}"
USE_TERRAFORM="false"
DESTROY_MODE="false"
ARG_PROJECT_ID=""

for arg in "$@"; do
  case "$arg" in
    --cli)
      USE_TERRAFORM="false"
      ;;
    --terraform)
      USE_TERRAFORM="true"
      ;;
    --destroy)
      DESTROY_MODE="true"
      USE_TERRAFORM="true"
      ;;
    --help|-h)
      echo "Uso: ./setup_raw.sh [PROJECT_ID] [--terraform] [--destroy]"
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
echo " Retail Open Lakehouse — Setup Turbo Camada Raw + Workspaces Paralelos"
echo " Project ID : ${PROJECT_ID}"
echo " Região     : ${REGION}"
echo "======================================================================"

if [[ "${USE_TERRAFORM}" == "true" ]]; then
  if ! command -v terraform >/dev/null 2>&1; then
    echo "ERRO: Binário 'terraform' não encontrado. Rode './setup_raw.sh' sem '--terraform' para usar o motor rápido via API." >&2
    exit 1
  fi

  if ! gcloud auth application-default print-access-token >/dev/null 2>&1; then
    echo "-> Credenciais ADC não encontradas. Abrindo login..."
    gcloud auth application-default login
  fi

  terraform -chdir="${SCRIPT_DIR}/terraform" init -input=false

  if [[ "${DESTROY_MODE}" == "true" ]]; then
    terraform -chdir="${SCRIPT_DIR}/terraform" destroy -auto-approve -var="project_id=${PROJECT_ID}" -var="region=${REGION}"
    exit 0
  fi

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
  echo "-> Executando motor paralelo ultra-rápido via Google Cloud REST APIs (18 workers)..."
  python3 "${SCRIPT_DIR}/scripts/provision_raw_fallback.py" --project-id "${PROJECT_ID}" --region "${REGION}"
fi
