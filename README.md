# Workshop Agentic Data — Retail Open Lakehouse (`raw_staging_parquet`)

Repositório de dados brutos (**Camada Raw Iceberg-Ready**) para o workshop hands-on de produtividade em Engenharia e Ciência de Dados com **Google Antigravity**, **Data Agent Kit**, **BigQuery Studio Pipelines**, **BigLake Apache Iceberg** e **Knowledge Catalog**.

---

## Estrutura do Repositório

```text
workshop-agentic-data/
├── README.md
└── raw_staging_parquet/
    ├── iceberg_tables_manifest.json    # Manifesto declarativo (schemas, PARTITION BY e CLUSTER BY das 19 tabelas)
    └── *.parquet                       # 19 arquivos Parquet SNAPPY unificados prontos para carga em BigLake Iceberg
```

## Por que este pacote é "Iceberg-Ready"?

1. **Shards já unificados em Parquet (`SNAPPY`):** Os 28 shards originais de domínio de varejo (PDV, estoque, compras, transferências, frota, fornecedores, produtos e clientes) foram consolidados em **19 arquivos `.parquet`** únicos e leves.
2. **Pronto para `PARTITION BY` + `CLUSTER BY` no BigLake Iceberg:**
   - Tabelas BigLake Apache Iceberg no BigQuery não permitem evolução de partição após criadas e exigem colunas `DATE`, `DATETIME` ou `TIMESTAMP` na cláusula `PARTITION BY`.
   - As **8 tabelas temporais** já possuem exclusivamente sua coluna de partição tipada como `DATE` (`DATE32` no Parquet), mantendo **todas as demais colunas brutas como `STRING`** (preservando dados brutos, zeros à esquerda e anomalias reais de negócio — como `zip_cd` vs `zip_code` e `received_date` nulo em pedidos `PENDING` — para serem tratadas nos pipelines posteriores).
   - As **11 tabelas cadastrais e de itens** possuem todas as colunas como `STRING` e especificação de `CLUSTER BY` para *block pruning* automático nos arquivos Parquet do Iceberg.
3. **Manifesto Declarativo (`iceberg_tables_manifest.json`):** Contém a lista exata das 19 tabelas, contagem esperada de linhas, schema (`DATE` / `STRING`), coluna de `partition_by` e colunas de `cluster_by` para provisionamento automatizado e determinístico no BigQuery.

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

## Fonte Original dos Dados
Adaptado de [GoogleCloudPlatform/retail-data-to-ai-workshop](https://github.com/GoogleCloudPlatform/retail-data-to-ai-workshop) sob licença Apache 2.0.
