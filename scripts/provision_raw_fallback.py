#!/usr/bin/env python3
"""Provisionamento paralelo e idempotente da Camada Raw BigLake Apache Iceberg.

Executado automaticamente por `setup_raw.sh` quando o binário `terraform` não está
instalado na máquina local do participante (ou quando `--cli` é informado),
aplicando exatamente a mesma arquitetura declarada em `terraform/main.tf` e
`raw_staging_parquet/iceberg_tables_manifest.json`.
"""

import argparse
import concurrent.futures
import json
import os
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

REQUIRED_APIS = [
    "bigquery.googleapis.com",
    "biglake.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "dataform.googleapis.com",
    "dataplex.googleapis.com",
    "datalineage.googleapis.com",
    "dataproc.googleapis.com",
    "aiplatform.googleapis.com",
    "notebooks.googleapis.com",
    "cloudaicompanion.googleapis.com",
    "geminidataanalytics.googleapis.com",
    "storage.googleapis.com",
]


def gcloud_env() -> dict:
    env = os.environ.copy()
    cur = env.get("CLOUDSDK_METRICS_ENVIRONMENT", "").strip()
    tag = "datacloud.antigravity"
    if tag not in cur.split():
        env["CLOUDSDK_METRICS_ENVIRONMENT"] = f"{cur} {tag}".strip()
    return env


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True, env=gcloud_env())


def get_access_token() -> str:
    res = run_cmd(["gcloud", "auth", "print-access-token"])
    token = res.stdout.strip()
    if not token:
        raise RuntimeError("Não foi possível obter access token via gcloud. Execute 'gcloud auth login'.")
    return token


def api_json(url: str, token: str, method: str = "GET", payload: dict | None = None) -> tuple[int, dict]:
    ctx = ssl.create_default_context()
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else {})
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(err_body)
        except Exception:
            return e.code, {"raw_error": err_body}


def sql_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def run_bq_job(project_id: str, region: str, sql: str, token: str) -> None:
    job_id = f"raw_iceberg_{uuid.uuid4().hex[:12]}"
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs"
    payload = {
        "jobReference": {"projectId": project_id, "location": region, "jobId": job_id},
        "configuration": {
            "labels": {"datacloud": "antigravity", "layer": "raw", "format": "iceberg"},
            "query": {"query": sql, "useLegacySql": False},
        },
    }
    code, resp = api_json(url, token, method="POST", payload=payload)
    if code not in (200, 201):
        raise RuntimeError(f"Erro ao submeter job BigQuery ({code}): {resp}")

    poll_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs/{job_id}?location={region}"
    while True:
        p_code, p_resp = api_json(poll_url, token, method="GET")
        if p_code != 200:
            raise RuntimeError(f"Erro ao consultar status do job {job_id}: {p_resp}")
        state = p_resp.get("status", {}).get("state")
        if state == "DONE":
            err = p_resp.get("status", {}).get("errorResult")
            if err:
                raise RuntimeError(f"Job BigQuery {job_id} falhou: {err}")
            return
        time.sleep(1.5)


def ensure_connection(project_id: str, region: str, conn_id: str, token: str) -> str:
    base = f"https://bigqueryconnection.googleapis.com/v2/projects/{project_id}/locations/{region}/connections"
    code, resp = api_json(f"{base}/{conn_id}", token, method="GET")
    if code == 404:
        create_payload = {
            "friendlyName": "Retail Open Lakehouse Iceberg Connection",
            "description": "Cloud Resource Connection for BigLake Managed Apache Iceberg tables.",
            "cloudResource": {},
        }
        code, resp = api_json(f"{base}?connectionId={conn_id}", token, method="POST", payload=create_payload)
        if code not in (200, 201):
            raise RuntimeError(f"Falha ao criar conexão {conn_id} ({code}): {resp}")
    elif code != 200:
        raise RuntimeError(f"Falha ao consultar conexão {conn_id} ({code}): {resp}")

    sa_id = resp.get("cloudResource", {}).get("serviceAccountId")
    if not sa_id:
        raise RuntimeError(f"serviceAccountId ausente na conexão {conn_id}: {resp}")
    return sa_id


def ensure_dataset(project_id: str, region: str, dataset_id: str, description: str, token: str) -> None:
    base = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets"
    payload = {
        "datasetReference": {"projectId": project_id, "datasetId": dataset_id},
        "location": region,
        "description": description,
        "labels": {"datacloud": "antigravity", "layer": "raw", "format": "iceberg"},
    }
    code, resp = api_json(f"{base}/{dataset_id}", token, method="GET")
    if code == 404:
        c_code, c_resp = api_json(base, token, method="POST", payload=payload)
        if c_code not in (200, 201):
            raise RuntimeError(f"Falha ao criar dataset {dataset_id} ({c_code}): {c_resp}")
    elif code == 200:
        resp["description"] = description
        resp.setdefault("labels", {}).update({"datacloud": "antigravity", "layer": "raw", "format": "iceberg"})
        u_code, u_resp = api_json(f"{base}/{dataset_id}", token, method="PUT", payload=resp)
        if u_code != 200:
            raise RuntimeError(f"Falha ao atualizar dataset {dataset_id} ({u_code}): {u_resp}")
    else:
        raise RuntimeError(f"Falha ao inspecionar dataset {dataset_id} ({code}): {resp}")


def provision_one_table(
    project_id: str,
    region: str,
    dataset_id: str,
    bucket_name: str,
    conn_id: str,
    tbl: str,
    meta: dict,
    token: str,
) -> tuple[str, int]:
    if not IDENT_RE.match(tbl):
        raise ValueError(f"Identificador de tabela inválido: {tbl}")

    col_defs = []
    alter_cols = []
    for col in meta["columns"]:
        cname = col["name"]
        ctype = col["type"]
        cdesc = sql_escape(col.get("description", ""))
        if not IDENT_RE.match(cname) or ctype not in ("STRING", "DATE"):
            raise ValueError(f"Coluna inválida {tbl}.{cname} ({ctype})")
        col_defs.append(f'  `{cname}` {ctype} OPTIONS(description="{cdesc}")')
        alter_cols.append(f'ALTER TABLE `{project_id}.{dataset_id}.{tbl}` ALTER COLUMN `{cname}` SET OPTIONS(description="{cdesc}");')

    part_clause = ""
    if meta.get("partition_by"):
        pcol = meta["partition_by"]
        if not IDENT_RE.match(pcol):
            raise ValueError(f"Coluna de partição inválida: {pcol}")
        part_clause = f"\nPARTITION BY `{pcol}`"

    cluster_cols = ", ".join(f"`{c}`" for c in meta.get("cluster_by", []))
    cluster_clause = f"\nCLUSTER BY {cluster_cols}" if cluster_cols else ""
    tdesc = sql_escape(meta.get("description", ""))
    cols_sql = ",\n".join(col_defs)

    ddl_and_load = f"""
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{tbl}` (
{cols_sql}
){part_clause}{cluster_clause}
WITH CONNECTION `projects/{project_id}/locations/{region}/connections/{conn_id}`
OPTIONS (
  table_format = 'ICEBERG',
  file_format = 'PARQUET',
  storage_uri = 'gs://{bucket_name}/iceberg/raw/{tbl}',
  description = "{tdesc}",
  labels = [('datacloud', 'antigravity'), ('layer', 'raw'), ('format', 'iceberg')]
);

ALTER TABLE `{project_id}.{dataset_id}.{tbl}` SET OPTIONS (description = "{tdesc}");
{" ".join(alter_cols)}

TRUNCATE TABLE `{project_id}.{dataset_id}.{tbl}`;

LOAD DATA INTO `{project_id}.{dataset_id}.{tbl}`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://{bucket_name}/staging/{tbl}.parquet']
);

EXPORT TABLE METADATA FROM `{project_id}.{dataset_id}.{tbl}`;
"""
    run_bq_job(project_id, region, ddl_and_load, token)
    return tbl, meta["row_count"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Provisiona a camada Raw Iceberg via GCP APIs/CLI.")
    parser.add_argument("--project-id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--region", default="us-central1", help="Região GCP (default: us-central1)")
    parser.add_argument("--dataset-id", default="retail_raw", help="Dataset BigQuery (default: retail_raw)")
    parser.add_argument("--connection-id", default="retail_iceberg_conn", help="BigQuery Connection ID")
    args = parser.parse_args()

    if not PROJECT_ID_RE.match(args.project_id):
        sys.exit(f"ERRO: Project ID inválido: '{args.project_id}'.")

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    manifest_path = os.path.join(repo_root, "raw_staging_parquet", "iceberg_tables_manifest.json")
    manifest = json.load(open(manifest_path, encoding="utf-8"))
    bucket_name = f"{args.project_id}-retail-lakehouse"

    print(f"[1/6] Habilitando APIs essenciais no projeto '{args.project_id}'...")
    run_cmd(["gcloud", "services", "enable", *REQUIRED_APIS, f"--project={args.project_id}"])

    print(f"[2/6] Garantindo bucket Lakehouse 'gs://{bucket_name}' ({args.region}, sem versionamento)...")
    chk = run_cmd(["gcloud", "storage", "buckets", "describe", f"gs://{bucket_name}", f"--project={args.project_id}"], check=False)
    if chk.returncode != 0:
        run_cmd([
            "gcloud", "storage", "buckets", "create", f"gs://{bucket_name}",
            f"--project={args.project_id}",
            f"--location={args.region}",
            "--uniform-bucket-level-access",
        ])
    run_cmd(["gcloud", "storage", "buckets", "update", f"gs://{bucket_name}", "--no-versioning", f"--project={args.project_id}"])

    print("[3/6] Enviando os 19 arquivos Parquet Iceberg-Ready para gs://.../staging/...")
    parquet_dir = os.path.join(repo_root, "raw_staging_parquet")
    parquet_files = [os.path.join(parquet_dir, f"{t}.parquet") for t in sorted(manifest["tables"].keys())]
    run_cmd(["gcloud", "storage", "cp", *parquet_files, f"gs://{bucket_name}/staging/", f"--project={args.project_id}"])

    token = get_access_token()
    print(f"[4/6] Configurando Cloud Resource Connection '{args.connection_id}' e permissões IAM...")
    sa_email = ensure_connection(args.project_id, args.region, args.connection_id, token)
    iam_res = run_cmd([
        "gcloud", "storage", "buckets", "add-iam-policy-binding", f"gs://{bucket_name}",
        f"--member=serviceAccount:{sa_email}",
        "--role=roles/storage.admin",
        f"--project={args.project_id}",
    ])
    if "bindings" in iam_res.stdout or iam_res.returncode == 0:
        print(f"      Service Account '{sa_email}' vinculada. Aguardando 35s para propagação segura de IAM...")
        time.sleep(35)

    print(f"[5/6] Garantindo dataset '{args.dataset_id}' ({args.region}) já catalogado...")
    ensure_dataset(args.project_id, args.region, args.dataset_id, manifest["dataset_description"], token)

    print(f"[6/6] Criando, particionando/clusterizando, carregando e catalogando as 19 tabelas BigLake Apache Iceberg em paralelo...")
    token = get_access_token()
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(
                provision_one_table,
                args.project_id,
                args.region,
                args.dataset_id,
                bucket_name,
                args.connection_id,
                tbl,
                meta,
                token,
            ): tbl
            for tbl, meta in manifest["tables"].items()
        }
        for fut in concurrent.futures.as_completed(futures):
            tbl = futures[fut]
            t_name, r_cnt = fut.result()
            print(f"      [OK] {args.dataset_id}.{t_name:30s} ({r_cnt:6d} linhas | Iceberg + Partição/Cluster + Catalogação)")

    print("\n======================================================================")
    print(f"SUCESSO! Camada Raw Apache Iceberg pronta em `{args.project_id}.{args.dataset_id}` ({args.region})")
    print(f"Bucket Lakehouse: gs://{bucket_name}/iceberg/raw/")
    print("Todas as 19 tabelas e 136 colunas já estão catalogadas no BigQuery/Knowledge Catalog.")
    print("======================================================================")


if __name__ == "__main__":
    main()
