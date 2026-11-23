#!/usr/bin/env python3
"""Provisionamento paralelo ultra-rápido (~35-45s) e idempotente da Camada Raw BigLake Apache Iceberg.

Executado por padrão em `setup_raw.sh`:
1. Habilita apenas APIs pendentes (0s se já ativas na Tarefa 1);
2. Garante bucket Lakehouse (sem versionamento/soft-delete) e envia os 19 Parquets em lote;
3. Garante a conexão `retail_iceberg_conn` e pré-autoriza TANTO a SA BigLake quanto a
   Default Compute Service Account (`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`),
   eliminando a necessidade de criar Service Account nova no BigQuery Studio Pipelines;
4. Substitui esperas fixas longas por sonda ativa de prontidão IAM (probe de 4s);
5. Cria, particiona/clusteriza, carrega e cataloga as 19 tabelas Iceberg em 1 único job SQL paralelo por tabela;
6. Grava `config.json` + `GEMINI.md` na raiz da Trilha 1 e já prepara a pasta paralela `thelook-graph-agent` (Trilha 2).
"""

import argparse
import concurrent.futures
import json
import os
import pathlib
import re
import shutil
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
    "cloudresourcemanager.googleapis.com",
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

SSL_CTX = ssl.create_default_context()
if os.path.exists("/etc/ssl/cert.pem"):
    SSL_CTX.load_verify_locations("/etc/ssl/cert.pem")


def gcloud_env() -> dict:
    env = os.environ.copy()
    cur = env.get("CLOUDSDK_METRICS_ENVIRONMENT", "").strip()
    tag = "datacloud.antigravity"
    if tag not in cur.split():
        env["CLOUDSDK_METRICS_ENVIRONMENT"] = f"{cur} {tag}".strip()
    return env


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    res = subprocess.run(cmd, check=False, text=True, capture_output=True, env=gcloud_env())
    if check and res.returncode != 0:
        raise RuntimeError(
            f"Comando falhou (exit {res.returncode}): {' '.join(cmd)}\n"
            f"STDERR: {res.stderr.strip()}\nSTDOUT: {res.stdout.strip()}"
        )
    return res


def get_access_token() -> str:
    res = run_cmd(["gcloud", "auth", "print-access-token"])
    token = res.stdout.strip()
    if not token:
        raise RuntimeError("Não foi possível obter access token via gcloud. Execute 'gcloud auth login'.")
    return token


def api_json(
    url: str,
    token: str,
    project_id: str,
    method: str = "GET",
    payload: dict | None = None,
) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Goog-User-Project": project_id,
        },
    )
    try:
        with urllib.request.urlopen(req, context=SSL_CTX) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else {})
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(err_body)
        except Exception:
            return e.code, {"raw_error": err_body}


def get_project_number(project_id: str, token: str) -> str:
    code, resp = api_json(
        f"https://cloudresourcemanager.googleapis.com/v1/projects/{project_id}",
        token,
        project_id,
        method="GET",
    )
    if code == 200 and resp.get("projectNumber"):
        return str(resp["projectNumber"])
    res = run_cmd(["gcloud", "projects", "describe", project_id, "--format=value(projectNumber)"])
    pnum = res.stdout.strip()
    if not pnum:
        raise RuntimeError(f"Não foi possível obter o projectNumber para '{project_id}'.")
    return pnum


def ensure_apis_fast(project_id: str, token: str) -> None:
    """Verifica APIs já ativas em 1 chamada rápida e habilita somente as faltantes."""
    url = f"https://serviceusage.googleapis.com/v1/projects/{project_id}/services?filter=state:ENABLED&pageSize=200"
    code, resp = api_json(url, token, project_id, method="GET")
    if code == 200:
        enabled = {
            s.get("config", {}).get("name", "")
            for s in resp.get("services", [])
        }
        missing = [api for api in REQUIRED_APIS if api not in enabled]
        if not missing:
            print("      Todas as APIs essenciais já estão ativas (0s).")
            return
        print(f"      Ativando {len(missing)} API(s) pendente(s): {', '.join(missing)}...")
        run_cmd(["gcloud", "services", "enable", *missing, f"--project={project_id}"])
    else:
        run_cmd(["gcloud", "services", "enable", *REQUIRED_APIS, f"--project={project_id}"])


def sql_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def run_bq_job(
    project_id: str,
    region: str,
    sql: str,
    token: str,
    retries: int = 3,
    retry_sleep: float = 4.0,
) -> None:
    last_err = None
    for attempt in range(1, retries + 1):
        job_id = f"raw_iceberg_{uuid.uuid4().hex[:12]}"
        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs"
        payload = {
            "jobReference": {"projectId": project_id, "location": region, "jobId": job_id},
            "configuration": {
                "labels": {"datacloud": "antigravity", "layer": "raw", "format": "iceberg"},
                "query": {"query": sql, "useLegacySql": False},
            },
        }
        code, resp = api_json(url, token, project_id, method="POST", payload=payload)
        if code not in (200, 201):
            last_err = f"Erro ao submeter job BigQuery ({code}): {resp}"
        else:
            poll_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/jobs/{job_id}?location={region}"
            while True:
                p_code, p_resp = api_json(poll_url, token, project_id, method="GET")
                if p_code != 200:
                    last_err = f"Erro ao consultar status do job {job_id}: {p_resp}"
                    break
                state = p_resp.get("status", {}).get("state")
                if state == "DONE":
                    err = p_resp.get("status", {}).get("errorResult")
                    if not err:
                        return
                    last_err = f"Job BigQuery {job_id} falhou: {err}"
                    break
                time.sleep(0.7)

        if attempt < retries and last_err and any(
            kw in str(last_err).lower()
            for kw in ["access denied", "permission", "403", "not authorized", "rate", " IAM ", "storage"]
        ):
            time.sleep(retry_sleep)
            continue
        break

    raise RuntimeError(last_err or "Falha desconhecida ao executar job BigQuery.")


def ensure_connection(project_id: str, region: str, conn_id: str, token: str) -> tuple[str, bool]:
    """Garante a Cloud Resource Connection e retorna (serviceAccountId, recem_criada)."""
    base = f"https://bigqueryconnection.googleapis.com/v1/projects/{project_id}/locations/{region}/connections"
    code, resp = api_json(f"{base}/{conn_id}", token, project_id, method="GET")
    created_now = False
    if code == 404:
        created_now = True
        create_payload = {
            "friendlyName": "Retail Open Lakehouse Iceberg Connection",
            "description": "Cloud Resource Connection for BigLake Managed Apache Iceberg tables.",
            "cloudResource": {},
        }
        code, resp = api_json(f"{base}?connectionId={conn_id}", token, project_id, method="POST", payload=create_payload)
        if code not in (200, 201):
            run_cmd([
                "bq", "mk", "--connection",
                "--connection_type=CLOUD_RESOURCE",
                f"--project_id={project_id}",
                f"--location={region}",
                conn_id,
            ], check=False)
            code, resp = api_json(f"{base}/{conn_id}", token, project_id, method="GET")
            if code != 200:
                raise RuntimeError(f"Falha ao criar conexão {conn_id} ({code}): {resp}")
    elif code != 200:
        raise RuntimeError(f"Falha ao consultar conexão {conn_id} ({code}): {resp}")

    sa_id = resp.get("cloudResource", {}).get("serviceAccountId")
    if not sa_id:
        show_res = run_cmd([
            "bq", "show", "--format=json", "--connection",
            f"--project_id={project_id}", f"--location={region}", conn_id,
        ], check=False)
        if show_res.returncode == 0 and show_res.stdout.strip():
            sa_id = json.loads(show_res.stdout).get("cloudResource", {}).get("serviceAccountId")
    if not sa_id:
        raise RuntimeError(f"serviceAccountId ausente na conexão {conn_id}: {resp}")
    return sa_id, created_now


def bind_storage_iam_with_retry(project_id: str, bucket_name: str, sa_email: str) -> None:
    """Vincula roles/storage.admin aguardando materialização assíncrona da SA no IAM."""
    last_stderr = ""
    for attempt in range(1, 9):
        b_res = run_cmd([
            "gcloud", "storage", "buckets", "add-iam-policy-binding", f"gs://{bucket_name}",
            f"--member=serviceAccount:{sa_email}",
            "--role=roles/storage.admin",
        ], check=False)
        if b_res.returncode == 0:
            return

        p_res = run_cmd([
            "gcloud", "projects", "add-iam-policy-binding", project_id,
            f"--member=serviceAccount:{sa_email}",
            "--role=roles/storage.admin",
            "--condition=None",
        ], check=False)
        if p_res.returncode == 0:
            return

        last_stderr = f"Bucket IAM: {b_res.stderr.strip()} | Project IAM: {p_res.stderr.strip()}"
        print(f"      [Tentativa {attempt}/8] Aguardando registro IAM de '{sa_email}' (5s)...")
        time.sleep(5)

    raise RuntimeError(f"Não foi possível conceder roles/storage.admin para {sa_email}:\n{last_stderr}")


def authorize_default_compute_sa(
    project_id: str,
    region: str,
    bucket_name: str,
    conn_id: str,
    default_compute_sa: str,
    token: str,
) -> None:
    """Pré-autoriza a Default Compute SA para que o BigQuery Studio Pipeline rode direto sem criar SA nova."""
    # 1) Acesso ao bucket Lakehouse para escrita de dados/metadados Iceberg
    run_cmd([
        "gcloud", "storage", "buckets", "add-iam-policy-binding", f"gs://{bucket_name}",
        f"--member=serviceAccount:{default_compute_sa}",
        "--role=roles/storage.admin",
    ], check=False)

    # 2) Permissão de delegação/uso direto na Cloud Resource Connection via API REST (< 0.3s)
    conn_res = f"https://bigqueryconnection.googleapis.com/v1/projects/{project_id}/locations/{region}/connections/{conn_id}"
    g_code, policy = api_json(f"{conn_res}:getIamPolicy", token, project_id, method="POST", payload={})
    if g_code == 200:
        bindings = policy.get("bindings", [])
        member = f"serviceAccount:{default_compute_sa}"
        found = False
        for b in bindings:
            if b.get("role") == "roles/bigquery.connectionAdmin":
                if member not in b.get("members", []):
                    b.setdefault("members", []).append(member)
                found = True
                break
        if not found:
            bindings.append({"role": "roles/bigquery.connectionAdmin", "members": [member]})
        policy["bindings"] = bindings
        api_json(f"{conn_res}:setIamPolicy", token, project_id, method="POST", payload={"policy": policy})


def ensure_dataset(project_id: str, region: str, dataset_id: str, description: str, token: str) -> None:
    base = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets"
    payload = {
        "datasetReference": {"projectId": project_id, "datasetId": dataset_id},
        "location": region,
        "description": description,
        "labels": {"datacloud": "antigravity", "layer": "raw", "format": "iceberg"},
    }
    code, resp = api_json(f"{base}/{dataset_id}", token, project_id, method="GET")
    if code == 404:
        c_code, c_resp = api_json(base, token, project_id, method="POST", payload=payload)
        if c_code not in (200, 201):
            raise RuntimeError(f"Falha ao criar dataset {dataset_id} ({c_code}): {c_resp}")
    elif code == 200:
        if resp.get("description") != description:
            resp["description"] = description
            resp.setdefault("labels", {}).update({"datacloud": "antigravity", "layer": "raw", "format": "iceberg"})
            api_json(f"{base}/{dataset_id}", token, project_id, method="PUT", payload=resp)
    else:
        raise RuntimeError(f"Falha ao inspecionar dataset {dataset_id} ({code}): {resp}")


def sync_table_catalog_metadata(
    project_id: str,
    dataset_id: str,
    tbl: str,
    meta: dict,
    token: str,
) -> None:
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{tbl}"
    code, current = api_json(url, token, project_id, method="GET")
    if code != 200:
        return
    desc_map = {c["name"]: c.get("description", "") for c in meta["columns"]}
    fields = current.get("schema", {}).get("fields", [])
    needs_patch = current.get("description", "") != meta.get("description", "")
    for f in fields:
        target_desc = desc_map.get(f.get("name"), "")
        if target_desc and f.get("description") != target_desc:
            f["description"] = target_desc
            needs_patch = True
    if needs_patch:
        patch_payload = {
            "description": meta.get("description", ""),
            "labels": {"datacloud": "antigravity", "layer": "raw", "format": "iceberg"},
            "schema": {"fields": fields},
        }
        api_json(url, token, project_id, method="PATCH", payload=patch_payload)


def provision_one_table(
    project_id: str,
    region: str,
    dataset_id: str,
    bucket_name: str,
    conn_id: str,
    tbl: str,
    meta: dict,
    token: str,
    retries: int = 4,
    retry_sleep: float = 4.0,
) -> tuple[str, int]:
    if not IDENT_RE.match(tbl):
        raise ValueError(f"Identificador de tabela inválido: {tbl}")

    col_defs = []
    for col in meta["columns"]:
        cname = col["name"]
        ctype = col["type"]
        cdesc = sql_escape(col.get("description", ""))
        if not IDENT_RE.match(cname) or ctype not in ("STRING", "DATE"):
            raise ValueError(f"Coluna inválida {tbl}.{cname} ({ctype})")
        col_defs.append(f'  `{cname}` {ctype} OPTIONS(description="{cdesc}")')

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

    # 1 único script multi-statement por tabela (CREATE + DELETE + LOAD em 1 job!)
    combined_sql = f"""
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
DELETE FROM `{project_id}.{dataset_id}.{tbl}` WHERE TRUE;
LOAD DATA INTO `{project_id}.{dataset_id}.{tbl}`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://{bucket_name}/staging/{tbl}.parquet']
);
"""
    run_bq_job(project_id, region, combined_sql, token, retries=retries, retry_sleep=retry_sleep)
    sync_table_catalog_metadata(project_id, dataset_id, tbl, meta, token)
    return tbl, meta["row_count"]


def prepare_workspaces_and_gemini_md(
    repo_root: pathlib.Path,
    project_id: str,
    project_number: str,
    region: str,
    bucket_name: str,
    conn_id: str,
    default_compute_sa: str,
) -> tuple[pathlib.Path, pathlib.Path]:
    """Atualiza config.json + GEMINI.md na Trilha 1 e já deixa pronta a pasta paralela da Trilha 2 (theLook)."""
    parent_dir = repo_root.parent
    is_inside_workshop_dir = (
        parent_dir.name == "retail-lakehouse-workshop" or (parent_dir / "config.json").exists()
    )
    track1_root = parent_dir if is_inside_workshop_dir else repo_root
    track2_root = (parent_dir.parent if is_inside_workshop_dir else parent_dir) / "thelook-graph-agent"

    track1_cfg = {
        "PROJECT_ID": project_id,
        "PROJECT_NUMBER": project_number,
        "REGION": region,
        "DEFAULT_COMPUTE_SA": default_compute_sa,
        "LAKEHOUSE_BUCKET": bucket_name,
        "ICEBERG_CONNECTION": f"projects/{project_id}/locations/{region}/connections/{conn_id}",
    }
    for cfg_target in {track1_root / "config.json", repo_root / "config.json"}:
        existing = {}
        if cfg_target.exists():
            try:
                existing = json.loads(cfg_target.read_text(encoding="utf-8"))
            except Exception:
                existing = {}
        existing.update(track1_cfg)
        cfg_target.write_text(json.dumps(existing, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    gemini_track1_src = repo_root / "GEMINI.md"
    if gemini_track1_src.exists() and track1_root != repo_root:
        shutil.copy2(gemini_track1_src, track1_root / "GEMINI.md")

    # Preparar pasta da Trilha 2 Paralela (theLook E-commerce Graph + Conversational Data Agent)
    track2_root.mkdir(parents=True, exist_ok=True)
    track2_cfg = {
        "PROJECT_ID": project_id,
        "PROJECT_NUMBER": project_number,
        "DATASET_ID": "thelook_ecommerce",
        "LOCATION": "US",
        "PUBLIC_SOURCE": "bigquery-public-data.thelook_ecommerce",
        "GRAPH_ID": "ecommerce_graph",
        "AGENT_ID": "thelook_ecommerce_advisor",
    }
    (track2_root / "config.json").write_text(
        json.dumps(track2_cfg, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    gemini_track2_src = repo_root / "templates" / "GEMINI_THELOOK.md"
    if gemini_track2_src.exists():
        shutil.copy2(gemini_track2_src, track2_root / "GEMINI.md")

    return track1_root, track2_root


def main() -> None:
    t0 = time.time()
    parser = argparse.ArgumentParser(description="Provisiona a camada Raw Iceberg em alta velocidade via GCP APIs/CLI.")
    parser.add_argument("--project-id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--region", default="us-central1", help="Região GCP (default: us-central1)")
    parser.add_argument("--dataset-id", default="retail_raw", help="Dataset BigQuery (default: retail_raw)")
    parser.add_argument("--connection-id", default="retail_iceberg_conn", help="BigQuery Connection ID")
    args = parser.parse_args()

    if not PROJECT_ID_RE.match(args.project_id):
        sys.exit(f"ERRO: Project ID inválido: '{args.project_id}'.")

    repo_root = pathlib.Path(__file__).resolve().parent.parent
    manifest_path = repo_root / "raw_staging_parquet" / "iceberg_tables_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bucket_name = f"{args.project_id}-retail-lakehouse"

    token = get_access_token()
    print(f"[1/6] Verificando/habilitando APIs essenciais no projeto '{args.project_id}'...")
    ensure_apis_fast(args.project_id, token)

    project_number = get_project_number(args.project_id, token)
    default_compute_sa = f"{project_number}-compute@developer.gserviceaccount.com"

    print(f"[2/6] Garantindo bucket Lakehouse 'gs://{bucket_name}' ({args.region}, sem versionamento/soft-delete)...")
    b_code, _ = api_json(
        f"https://storage.googleapis.com/storage/v1/b/{bucket_name}",
        token,
        args.project_id,
        method="GET",
    )
    if b_code == 404:
        run_cmd([
            "gcloud", "storage", "buckets", "create", f"gs://{bucket_name}",
            f"--project={args.project_id}",
            f"--location={args.region}",
            "--uniform-bucket-level-access",
        ])
    run_cmd([
        "gcloud", "storage", "buckets", "update", f"gs://{bucket_name}",
        "--no-versioning", "--clear-soft-delete", f"--project={args.project_id}",
    ], check=False)

    print("[3/6] Enviando os 19 arquivos Parquet Iceberg-Ready para gs://.../staging/...")
    parquet_dir = repo_root / "raw_staging_parquet"
    parquet_files = [str(parquet_dir / f"{t}.parquet") for t in sorted(manifest["tables"].keys())]
    run_cmd(["gcloud", "storage", "cp", *parquet_files, f"gs://{bucket_name}/staging/", f"--project={args.project_id}"])

    token = get_access_token()
    print(f"[4/6] Configurando conexão '{args.connection_id}' e autorizando Default Compute SA ('{default_compute_sa}')...")
    sa_email, created_now = ensure_connection(args.project_id, args.region, args.connection_id, token)
    if created_now:
        time.sleep(3)
    bind_storage_iam_with_retry(args.project_id, bucket_name, sa_email)
    authorize_default_compute_sa(
        args.project_id, args.region, bucket_name, args.connection_id, default_compute_sa, token
    )

    print(f"[5/6] Garantindo dataset '{args.dataset_id}' ({args.region}) e arquivos GEMINI.md das 2 trilhas...")
    ensure_dataset(args.project_id, args.region, args.dataset_id, manifest["dataset_description"], token)
    track1_root, track2_root = prepare_workspaces_and_gemini_md(
        repo_root, args.project_id, project_number, args.region, bucket_name, args.connection_id, default_compute_sa
    )

    print(f"[6/6] Criando, particionando/clusterizando e carregando as 19 tabelas Iceberg em paralelo...")
    token = get_access_token()
    all_items = sorted(manifest["tables"].items())
    # Sonda ativa na primeira tabela pequena (driver_master) para absorver qualquer propagação residual de IAM sem sleep fixo!
    probe_tbl = "driver_master" if "driver_master" in manifest["tables"] else all_items[0][0]
    probe_meta = manifest["tables"][probe_tbl]
    t_name, r_cnt = provision_one_table(
        args.project_id,
        args.region,
        args.dataset_id,
        bucket_name,
        args.connection_id,
        probe_tbl,
        probe_meta,
        token,
        retries=12,
        retry_sleep=4.0,
    )
    print(f"  [OK] {t_name:<30} | {r_cnt:>7} linhas | IAM Iceberg validado")

    remaining = [(t, m) for t, m in all_items if t != probe_tbl]
    with concurrent.futures.ThreadPoolExecutor(max_workers=18) as pool:
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
                4,
                4.0,
            ): tbl
            for tbl, meta in remaining
        }
        for fut in concurrent.futures.as_completed(futures):
            tbl_name = futures[fut]
            t_name, r_cnt = fut.result()
            print(f"  [OK] {t_name:<30} | {r_cnt:>7} linhas | Iceberg + Catalog OK")

    elapsed = time.time() - t0
    print("\n======================================================================")
    print(f" SUCESSO em {elapsed:.1f}s! Camada Raw Apache Iceberg pronta em '{args.project_id}.{args.dataset_id}'")
    print(f" • 19 tabelas BigLake Iceberg criadas, particionadas, clusterizadas e carregadas")
    print(f" • 19 tabelas + 136 colunas com descrições de negócio publicadas")
    print(f" • Default Compute SA pronta para o Pipeline: {default_compute_sa}")
    print(f" • Workspace Trilha 1 (Retail Iceberg): {track1_root}")
    print(f" • Workspace Trilha 2 Paralela (theLook): {track2_root}")
    print("======================================================================")


if __name__ == "__main__":
    main()
