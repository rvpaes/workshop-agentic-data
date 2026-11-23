# GEMINI.md — Diretrizes Arquiteturais da Trilha 1 (Retail Open Lakehouse: Iceberg + Pipelines + Graph + TimesFM 3.0)

Você está operando no workspace da **Trilha 1 (`retail-lakehouse-workshop`)**. Siga **obrigatoriamente** todas as regras abaixo em qualquer criação ou evolução de tabelas, pipelines, grafos e modelos preditivos, mesmo quando o prompt do usuário for resumido.

---

## 1. Contexto do Ambiente & Proibição Estrita de Criar Service Account Nova
1. Leia imediatamente o arquivo local `config.json` (na raiz do workspace ou em `workshop-agentic-data/config.json`) para obter:
   - `PROJECT_ID`
   - `PROJECT_NUMBER`
   - `REGION`: sempre `us-central1`
   - `DEFAULT_COMPUTE_SA`: `<PROJECT_NUMBER>-compute@developer.gserviceaccount.com` (já pré-autorizada com acesso ao BigQuery, bucket GCS e conexão BigLake!)
   - `LAKEHOUSE_BUCKET`: `<PROJECT_ID>-retail-lakehouse`
   - `ICEBERG_CONNECTION`: `projects/<PROJECT_ID>/locations/us-central1/connections/retail_iceberg_conn`
2. **REGRA DE VELOCIDADE Nº 1 — ZERO CRIAÇÃO DE SERVICE ACCOUNT:**
   - **É TERMINANTEMENTE PROIBIDO** executar `gcloud iam service-accounts create` ou aguardar propagação de IAM para contas novas.
   - Em `dak-dataform` (`create_repository` e `workflow_settings.yaml`), configure **SEMPRE** `serviceAccount` diretamente com a `DEFAULT_COMPUTE_SA` (`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`) que já existe nativa no projeto!
3. **REGRA DE VELOCIDADE Nº 2 — ZERO QUERIES EXPLORATÓRIAS EM `retail_raw`:**
   - Para conhecer os schemas, tipos, partições, clusters e descrições das 19 tabelas brutas, leia apenas o arquivo local `workshop-agentic-data/raw_staging_parquet/iceberg_tables_manifest.json` (ou `./raw_staging_parquet/iceberg_tables_manifest.json`). Não faça queries exploratórias tabela a tabela.

---

## 2. Padrão Obrigatório BigLake Apache Iceberg, FinOps & Qualidade SQL
1. Todas as tabelas de `retail_silver`, `retail_gold` e `retail_ml` devem ser criadas como **BigLake Managed Apache Iceberg** em `us-central1` (https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#use_partitioning):
   ```sql
   CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<dataset>.<tabela>` (
     `col_id` STRING OPTIONS(description="Descrição executiva de negócio..."),
     ...
   )
   [PARTITION BY <coluna_DATE>]
   CLUSTER BY <colunas_chave>
   WITH CONNECTION `projects/<PROJECT_ID>/locations/us-central1/connections/retail_iceberg_conn`
   OPTIONS (
     table_format = 'ICEBERG',
     file_format = 'PARQUET',
     storage_uri = 'gs://<PROJECT_ID>-retail-lakehouse/iceberg/<camada>/<tabela>',
     description = 'Descrição executiva da tabela...'
   );
   ```
2. **DML Idempotente em Iceberg:** `TRUNCATE TABLE` e `LOAD DATA OVERWRITE` não são suportados em BigLake Iceberg. Use sempre `DELETE FROM ... WHERE TRUE;` seguido de `INSERT INTO ... SELECT ...` com colunas explícitas.
3. **Boas Práticas Obrigatórias de Performance e Custo do BigQuery:**
   - Siga https://cloud.google.com/bigquery/docs/best-practices-query-performance e https://cloud.google.com/bigquery/docs/best-practices-cost.
   - **Proibido `SELECT *`**. Aplique poda de partição e cluster e **critique cada query SQL gerada** garantindo a versão mais otimizada em bytes/slots antes de gravar no pipeline.
4. **Governança Knowledge Catalog:**
   - Toda tabela e view criada deve conter descrições de negócio em português (`OPTIONS(description=...)`) na tabela e em **100% das colunas**.
   - Em `retail_silver` (camada intermediária): use apenas catalogação inline via `OPTIONS(description=...)` (sem Data Profile Scan nem `EXPORT TABLE METADATA`, para máxima velocidade).
   - Em `retail_gold` e `retail_ml` (camadas analíticas finais): execute `EXPORT TABLE METADATA`, publique **Data Profile Scan** (`LIGHTWEIGHT`, `dataplex-dp-published-scan`) e gere **Data Insights** publicados no Knowledge Catalog.
5. **Protocolo de Erros 3x SQL & Anti-Alucinação (`0 rows`):**
   - Se a API do BigQuery retornar erro de sintaxe SQL 3 vezes seguidas, pare de tentar adivinhar, retorne mensagem padrão informando que a consulta analítica falhou e pesquise usando o Gemini 3.1 como solucionar o erro.
   - Se uma consulta retornar `0 rows`, afirme explicitamente que não há dados para o período/critério selecionado — nunca invente nem extrapole tendências.

---

## 3. Regra Cumulativa do BigQuery Studio Pipeline (`retail-data-to-ai-pipeline`)
1. Repositório `dak-dataform` em `projects/<PROJECT_ID>/locations/us-central1`:
   - `repositoryId`: `"retail-data-to-ai-pipeline"`
   - `displayName`: `"Retail Data-to-AI Pipeline"`
   - `serviceAccount`: `<DEFAULT_COMPUTE_SA>`
   - `setAuthenticatedUserAdmin`: `true`
   - `labels`: `{"bigquery-workflow": "preview", "origin": "bigquery-studio"}`
2. Sincronize sempre todos os arquivos (`workflow_settings.yaml`, `GEMINI.md`, `definitions/actions.yaml`, `.sql`, `.ipynb`) nos workspaces `default` e de rascunho do usuário (`<email_sanitizado>-agent-draft`).
3. **Nomes de tarefas sem ponto (`.`):** use apenas letras minúsculas, números e `_` (ex: `task_silver_pos_sales`).
4. **PROIBIDO SOBRESCREVER TAREFAS ANTERIORES:** Ao evoluir o pipeline para `Gold` ou `ML`, leia obrigatoriamente `definitions/actions.yaml` via `dak-dataform:read_file` ANTES de `write_file` e **apenas anexe (`append`)** os novos nós mantendo intactos todos os nós `task_silver_*` e `task_gold_*` existentes!

---

## 4. Especificação Enxuta por Etapa da Trilha 1

### A) Camada Silver (`retail_silver` — 5 tarefas no DAG)
- Agrupe as 19 tabelas em **exatamente 4 tasks `operation:` paralelas por domínio + 1 task de Data Quality** (proibido criar 19 arquivos separados):
  1. `task_silver_master_entities` (`silver_master_entities.sql`): `customer_master`, `product_master`, `product_suppliers`, `product_docs_ref_data`, `supplier_master` (`zip_cd` → `zip_code`), `location_master`, `driver_master`, `fleet_master`, `forecast_override_configs` (`PARTITION BY last_update_date`);
  2. `task_silver_pos_sales` (`silver_pos_sales.sql`): `pos_transactions` (`PARTITION BY transaction_date`) e `pos_transaction_items` (`CLUSTER BY transaction_id, item_number`);
  3. `task_silver_inventory` (`silver_inventory.sql`): `stock_master` (`PARTITION BY stock_date`), `stock_master_location` (`PARTITION BY stock_date`), `stock_movement` (`PARTITION BY movement_date`) e `stock_thresholds` (`PARTITION BY last_updated_date`);
  4. `task_silver_supply_chain` (`silver_supply_chain.sql`): `stock_purchase_orders` (`PARTITION BY order_date`, `SAFE_CAST(received_date AS DATE)`), `stock_purchase_order_items`, `stock_transfer_orders` (`PARTITION BY transfer_order_date`, `SAFE_CAST(fulfillment_date AS DATE)`), `stock_transfer_order_items`;
  5. `task_silver_quality_gate` (`silver_quality_gate.sql`, dependente das 4 acima): valida com `ASSERT` chaves primárias não nulas e valores/quantidades não negativos.

### B) Camada Gold + Views + Property Graph (`retail_gold` — anexado após `task_silver_quality_gate`)
- `task_gold_dimensions` (`gold_dimensions.sql`): 5 dimensões Iceberg com `PRIMARY KEY (...) NOT ENFORCED` e `CLUSTER BY`:
  - `dim_products` (`KEY item_number`, `CLUSTER BY appliance_type, brand, item_number`)
  - `dim_customers` (`KEY customer_id`, `CLUSTER BY state_code, city, customer_id`)
  - `dim_locations` (`KEY location_id`, `CLUSTER BY location_type, state_code, location_id`)
  - `dim_suppliers` (`KEY supplier_id`, `CLUSTER BY state_code, supplier_id`)
  - `dim_fleet_drivers` (`PRIMARY KEY (vehicle_id) NOT ENFORCED` associando motorista titular por `vehicle_id`, `CLUSTER BY status, vehicle_id`)
- `task_gold_facts` (`gold_facts.sql`, dependente de `task_gold_dimensions`): 3 fatos Iceberg com `FOREIGN KEY (...) NOT ENFORCED`, `PARTITION BY` e `CLUSTER BY`:
  - `fct_sales_line_items` (`PARTITION BY transaction_date CLUSTER BY location_id, item_number, customer_id`)
  - `fct_inventory_daily_health` (`PARTITION BY stock_date CLUSTER BY location_id, item_number, is_below_safety_stock`)
  - `fct_supply_chain_orders` (unifica compras e transferências preservando `supplier_id`, `vehicle_id`, `from_location_id`, `location_id` destino e `item_number`, `PARTITION BY order_date CLUSTER BY location_id, item_number, supplier_id, order_status`)
- `task_gold_business_knowledge_graph` (`gold_knowledge_graph.sql`, dependente de `task_gold_facts`):
  - Cria `CREATE OR REPLACE PROPERTY GRAPH \`<PROJECT_ID>.retail_gold.retail_knowledge_graph\`` seguindo https://docs.cloud.google.com/bigquery/docs/graph-query-best-practices (propriedades explícitas escopadas, sem `PROPERTIES ALL COLUMNS`, sem palavras reservadas GQL como `Order` ou `Contains`).
  - **REGRA INEGOCIÁVEL — ZERO NÓS ÓRFÃOS/SEM CONEXÃO:** Nenhum `NODE TABLE` pode ficar sem aresta! Os **5 nós** (`Product`, `Customer`, `Location`, `Supplier`, `FleetDriver`) devem estar 100% conectados pelas **6 arestas**:
    - `PurchasedAtLocation` (`Customer` → `Location` em `fct_sales_line_items`)
    - `SoldProduct` (`Location` → `Product` em `fct_sales_line_items`)
    - `HoldsInventory` (`Location` → `Product` em `fct_inventory_daily_health`)
    - `SuppliedOrder` (`Supplier` → `Location` em `fct_supply_chain_orders` onde `supplier_id IS NOT NULL`)
    - `OrderedProduct` (`Supplier` → `Product` em `fct_supply_chain_orders` onde `supplier_id IS NOT NULL`)
    - `TransportsToLocation` (`FleetDriver` via `vehicle_id` → `Location` destino em `fct_supply_chain_orders` onde `vehicle_id IS NOT NULL`)
  - Valide via query GQL/`GRAPH_TABLE` que todos os 5 nós possuem conexões ativas (`> 0`).
- `task_gold_lakehouse_iceberg_views` (`notebook:` PySpark sobre BigLake Iceberg REST Catalog): cria `vw_executive_retail_kpis` e `vw_inventory_replenishment_alerts`.

### C) Camada Preditiva Multivariada TimesFM 3.0 + BigFrames (`retail_ml` — anexado após `task_gold_business_knowledge_graph`)
- `task_ml_timesfm3_graph_forecast`: extrai séries históricas e covariáveis via `GRAPH_TABLE` sobre `retail_gold.retail_knowledge_graph` e executa `AI.FORECAST(..., model => 'TimesFM 3.0', timestamp_col => 'forecast_date', id_cols => ['location_id', 'item_number'], target_cols => ['daily_units_sold', 'daily_net_revenue'], past_covariate_cols => [...], future_covariate_cols => [...], horizon => 30, confidence_level => 0.95)`, gravando em `retail_ml.fct_multivariate_forecast_timesfm3` (Iceberg particionado por `forecast_date`).
- `task_ds_stockout_and_rfm_notebook`: notebook `bigframes.pandas` em pushdown (sem `.to_pandas()` em grandes volumes) gerando `retail_ml.customer_rfm_segments` e `retail_ml.predictive_replenishment_recommendations` (Iceberg).
- `task_gold_enrich_graph_with_ml`: atualiza `retail_gold.retail_knowledge_graph` mantendo os 5 nós e 6 arestas originais e adicionando a aresta preditiva `NeedsReplenishment` (`Location` → `Product`).
