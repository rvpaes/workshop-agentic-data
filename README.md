# Workshop Agentic Data — Retail Open Lakehouse (`raw_staging_parquet` + Terraform)

Repositório oficial de bootstrap da **Camada Raw Iceberg-Ready** para o workshop hands-on de produtividade em Engenharia e Ciência de Dados com **Google Antigravity**, **Data Agent Kit**, **BigQuery Studio Pipelines**, **BigLake Apache Iceberg**, **BigQuery Knowledge Graph**, **TimesFM 3.0** e **Knowledge Catalog**.

---

## Provisionamento em 1 Comando (Via Terminal)

Para criar automaticamente o bucket GCS (sem versionamento e sem *Soft Delete*), a conexão BigLake (`retail_iceberg_conn`), o dataset `retail_raw` e carregar em paralelo as **19 tabelas BigLake Apache Iceberg** (já particionadas, clusterizadas, com snapshots `metadata.json` exportados e **100% das 19 tabelas e 136 colunas catalogadas com descrições de negócio**):

```bash
git clone https://github.com/rvpaes/workshop-agentic-data.git && cd workshop-agentic-data && ./setup_raw.sh
```

> **Idempotência / Retomada em caso de atualização:**
> Se o diretório já foi clonado na máquina ou VM do laboratório, basta executar:
> ```bash
> git pull && ./setup_raw.sh
> ```

### Opções Suportadas pelo `./setup_raw.sh`

* `./setup_raw.sh` — Detecta automaticamente o `PROJECT_ID` (`config.json` local/pai ou `gcloud config get-value project`) e executa via **Terraform** (`terraform/`) se instalado (ex: Cloud Shell), ou faz **fallback paralelo automático** via **Google Cloud APIs/CLI** (`scripts/provision_raw_fallback.py`) em máquinas/VMs sem Terraform (ex: containers Desktop do Qwiklabs).
* `./setup_raw.sh SEU_PROJECT_ID` — Define explicitamente o projeto Google Cloud alvo.
* `./setup_raw.sh --cli` — Força o provisionamento paralelo direto via APIs/CLI mesmo quando o Terraform estiver instalado.
* `./setup_raw.sh --destroy` — Remove toda a infraestrutura da camada Raw gerenciada pelo Terraform.

### Uso Direto via Terraform Puro (Opcional)

```bash
cd terraform
terraform init
terraform apply -auto-approve -var="project_id=SEU_PROJECT_ID"
```

---

## Arquitetura e Garantias Técnicas (BigLake Iceberg & Qwiklabs Ready)

1. **Bucket Lakehouse Compatível com Iceberg:** Configurado em `us-central1` com `uniform_bucket_level_access = true`, **versionamento de objetos desativado** (`--no-versioning`) e **política de Soft Delete limpa** (`--clear-soft-delete` / `retention_duration_seconds = 0`), evitando conflitos de retenção na reescrita de metadados Iceberg.
2. **Conexão BigLake Resiliente (`bigqueryconnection` v1 + CLI Fallback):** Provisiona `retail_iceberg_conn` via endpoint oficial `v1` com header `X-Goog-User-Project` (compatível com credenciais de aluno do Qwiklabs) e fallback via `bq mk --connection`, aguardando propagação de IAM (`roles/storage.admin`) antes de criar as tabelas.
3. **DML Idempotente Oficial para Iceberg:** Utiliza `CREATE TABLE IF NOT EXISTS` + `DELETE FROM ... WHERE TRUE` + `LOAD DATA INTO ... FROM FILES (format = 'PARQUET')` (visto que `TRUNCATE TABLE` e `LOAD DATA OVERWRITE` não são suportados em tabelas BigLake Apache Iceberg gerenciadas).
4. **Exportação Resiliente de Metadados (`EXPORT TABLE METADATA`):** Dispara a geração dos snapshots `metadata.json` abertos no GCS e trata graciosamente ambientes de sandbox/Qwiklabs que restrinjam *slots* Enterprise de `EXPORT_METADATA`, mantendo o snapshot gerenciado 100% ativo no BigQuery.
5. **Catalogação Automática End-to-End:** Sincroniza descrições executivas de negócio no dataset `retail_raw`, nas **19 tabelas** e em todas as **136 colunas** via DDL e API `tables.patch`, disponibilizando o contexto imediatamente para o **Knowledge Catalog** e para os agentes das próximas etapas.

---

## Estrutura do Repositório

```text
workshop-agentic-data/
├── README.md
├── setup_raw.sh                        # Bootstrap em 1 comando (Terraform com fallback automático CLI/API)
├── terraform/                          # Infraestrutura como Código (IaC) declarativa da Camada Raw Iceberg
│   ├── versions.tf
│   ├── variables.tf
│   ├── main.tf                         # Bucket, Connection, IAM, Dataset, 19 tabelas Iceberg e Jobs de carga
│   └── outputs.tf
├── scripts/
│   └── provision_raw_fallback.py       # Provisionador paralelo sem dependências externas (Qwiklabs/VM/Local)
└── raw_staging_parquet/
    ├── iceberg_tables_manifest.json    # Manifesto declarativo (schemas, 136 descrições, PARTITION BY e CLUSTER BY)
    └── *.parquet                       # 19 arquivos Parquet SNAPPY unificados prontos para BigLake Iceberg
```

---

## Catálogo das 19 Tabelas Raw (`retail_raw`)

| Tabela | Tipo | `PARTITION BY` | `CLUSTER BY` |
| :--- | :--- | :--- | :--- |
| `pos_transactions` | Temporal | `transaction_date` | `location_id, customer_id, transaction_status` |
| `stock_master` | Temporal | `stock_date` | `item_number, omni_item_id` |
| `stock_master_location` | Temporal | `stock_date` | `location_id, item_number` |
| `stock_movement` | Temporal | `movement_date` | `location_id, item_number, movement_type` |
| `stock_purchase_orders` | Temporal | `order_date` | `supplier_id, order_status` |
| `stock_thresholds` | Temporal | `last_updated_date` | `item_number, is_current` |
| `stock_transfer_orders` | Temporal | `transfer_order_date` | `from_location_id, to_location_id, status` |
| `forecast_override_configs` | Temporal | `last_update_date` | `item_number, omni_item_id` |
| `pos_transaction_items` | Itens | — | `transaction_id, item_number` |
| `stock_purchase_order_items` | Itens | — | `po_id, item_number` |
| `stock_transfer_order_items` | Itens | — | `stock_transfer_order_id, item_number` |
| `customer_master` | Cadastro | — | `state_code, city, customer_id` |
| `product_master` | Cadastro | — | `appliance_type, brand, item_number` |
| `product_suppliers` | Cadastro | — | `item_number, supplier_id` |
| `product_docs_ref_data` | Cadastro | — | `item_number, brand, product_doc_type` |
| `supplier_master` | Cadastro | — | `state_code, supplier_id` |
| `location_master` | Cadastro | — | `location_type, state_code, location_id` |
| `driver_master` | Cadastro | — | `driver_id, employee_id` |
| `fleet_master` | Cadastro | — | `status, vehicle_id` |

---

## Solução de Problemas (Troubleshooting)

* **`USER_PROJECT_DENIED` ou falta de credenciais no `gcloud`:**
  Execute `gcloud auth login` e `gcloud auth application-default set-quota-project "$(gcloud config get-value project)"`, e rode novamente `./setup_raw.sh`.
* **Propagação de IAM da conexão `retail_iceberg_conn`:**
  O script aguarda automaticamente a propagação de `roles/storage.admin` no bucket GCS. Caso em algum ambiente a propagação leve mais tempo na 1ª tentativa, basta reexecutar `./setup_raw.sh` (100% idempotente).

---

## Fonte Original dos Dados
Adaptado de [GoogleCloudPlatform/retail-data-to-ai-workshop](https://github.com/GoogleCloudPlatform/retail-data-to-ai-workshop) sob licença Apache 2.0.
