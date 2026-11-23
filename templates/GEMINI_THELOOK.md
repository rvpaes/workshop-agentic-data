# GEMINI.md — Diretrizes da Trilha 2 Paralela (theLook E-Commerce: BigQuery Graph + Conversational Data Agent)

Você está operando no workspace da **Trilha 2 (`thelook-graph-agent`)**, executado em paralelo enquanto o pipeline Lakehouse da Trilha 1 processa em background. Siga **obrigatoriamente** todas as regras abaixo, mantendo a experiência rápida, limpa e 100% aderente às boas práticas do BigQuery, Knowledge Catalog e Conversational Analytics.

---

## 1. Contexto do Ambiente e Fonte Pública (`US` Multi-Region)
1. Leia `config.json` na raiz deste workspace (ou obtenha `PROJECT_ID` via `gcloud config get-value project`).
2. **Localização Obrigatória `US`:** O dataset público `bigquery-public-data.thelook_ecommerce` reside na multi-região **`US`**. Portanto, crie o dataset `<PROJECT_ID>.thelook_ecommerce` **obrigatoriamente em `location = 'US'`** (assim a cópia das tabelas roda em ~3 segundos sem erro cross-region e convive perfeitamente com os datasets `retail_*` que estão em `us-central1`).
3. **Escopo Enxuto e Rápido (5 Tabelas Conectadas — ~42 MB):**
   Copie de `bigquery-public-data.thelook_ecommerce` para `<PROJECT_ID>.thelook_ecommerce` as **5 tabelas centrais** (NÃO copie a tabela pesada `events` de 368 MB a menos que explicitamente solicitado):
   - `distribution_centers` (`PRIMARY KEY (id) NOT ENFORCED`, `CLUSTER BY id, name`)
   - `users` (`PRIMARY KEY (id) NOT ENFORCED`, `CLUSTER BY country, state, id`)
   - `products` (`PRIMARY KEY (id) NOT ENFORCED`, `FOREIGN KEY (distribution_center_id) REFERENCES distribution_centers(id) NOT ENFORCED`, `CLUSTER BY category, brand, distribution_center_id`)
   - `orders` (`PARTITION BY DATE(created_at) CLUSTER BY status, user_id, order_id`, `PRIMARY KEY (order_id) NOT ENFORCED`, `FOREIGN KEY (user_id) REFERENCES users(id) NOT ENFORCED`)
   - `order_items` (`PARTITION BY DATE(created_at) CLUSTER BY status, order_id, product_id`, `PRIMARY KEY (id) NOT ENFORCED`, `FOREIGN KEY (order_id) REFERENCES orders(order_id) NOT ENFORCED`, `FOREIGN KEY (product_id) REFERENCES products(id) NOT ENFORCED`)
4. **Governança Obrigatória no Knowledge Catalog & FinOps BigQuery:**
   - Grave descrições executivas de negócio em português (`OPTIONS(description=...)`) no dataset, nas 5 tabelas e em **100% das colunas**.
   - Execute e publique no Knowledge Catalog o **Data Profile Scan** (`LIGHTWEIGHT`, label `dataplex-dp-published-scan`) e os **Data Insights** das tabelas criadas.
   - Siga https://cloud.google.com/bigquery/docs/best-practices-query-performance e https://cloud.google.com/bigquery/docs/best-practices-cost (proibido `SELECT *` em queries analíticas; critique sempre cada query gerada visando menor custo/bytes).

---

## 2. Regras do Property Graph (`thelook_ecommerce.ecommerce_graph`)
1. Siga estritamente https://docs.cloud.google.com/bigquery/docs/graph-query-best-practices e a skill `/bigquery-graph-author`.
2. **REGRA INEGOCIÁVEL — ZERO NÓS ÓRFÃOS/SEM CONEXÃO:**
   - **NUNCA deixe nenhum `NODE TABLE` sem conexão!** Todos os nós declarados devem participar em pelo menos uma `EDGE TABLE` com `> 0` arestas reais resolvidas:
     - **4 Nós (`NODE TABLES` com `PROPERTIES (...)` explícitas — proibido `PROPERTIES ALL COLUMNS`):**
       - `thelook_ecommerce.users AS Customer KEY(id)`
       - `thelook_ecommerce.orders AS CustomerOrder KEY(order_id)` *(ATENÇÃO: NUNCA use `LABEL Order`, pois `ORDER` é palavra reservada em GQL! Use sempre `CustomerOrder`)*
       - `thelook_ecommerce.products AS Product KEY(id)`
       - `thelook_ecommerce.distribution_centers AS DistributionCenter KEY(id)`
     - **3 Arestas Direcionadas (`EDGE TABLES` — evite a palavra reservada `Contains`, use `ContainsProduct`):**
       - `thelook_ecommerce.orders AS PlacedOrder` (`SOURCE KEY(user_id) REFERENCES Customer(id) DESTINATION KEY(order_id) REFERENCES CustomerOrder(order_id)`)
       - `thelook_ecommerce.order_items AS ContainsProduct` (`SOURCE KEY(order_id) REFERENCES CustomerOrder(order_id) DESTINATION KEY(product_id) REFERENCES Product(id)`)
       - `thelook_ecommerce.products AS StoredAtCenter` (`SOURCE KEY(id) REFERENCES Product(id) DESTINATION KEY(distribution_center_id) REFERENCES DistributionCenter(id)`)
3. Após criar o grafo, execute uma consulta GQL (`GRAPH_TABLE`) comprovando que os **4 nós (`Customer`, `CustomerOrder`, `Product`, `DistributionCenter`) estão 100% conectados**.

---

## 3. Regras do BigQuery Conversational Data Agent (`thelook_ecommerce_advisor`)
1. Siga https://docs.cloud.google.com/bigquery/docs/create-data-agents e as **7 Regras de Ouro da skill `/gcp_conversacional_analytics`** na API `geminidataanalytics.googleapis.com` (`locations/global`):
   - **Regra 1 — Exclusividade Mútua Estrita:** `datasourceReferences.bq` (em `stagingContext` e `publishedContext`) deve conter **UNICAMENTE** `propertyGraphReferences` apontando para `{"projectId": "<PROJECT_ID>", "datasetId": "thelook_ecommerce", "graphId": "ecommerce_graph"}`. **Proibido** misturar `tableReferences` no mesmo agente!
   - **Regra 2 — `systemInstruction` Executiva em Português:**
     - Respostas estruturadas em 4 blocos C-Level (*Resumo Executivo, Tabela/KPIs, Principais Insights & Drivers, Próximas Perguntas*), sem jargões técnicos (`GQL`, `MATCH`, `GRAPH_TABLE`).
     - **Estado Vazio (`0 rows`):** Se a consulta retornar 0 linhas, afirmar explicitamente que não há dados disponíveis para o período/critério selecionado e **nunca** inventar ou extrapolar tendências.
     - **Protocolo Anti-Loop 3x SQL/GQL:** Se a API do BigQuery retornar erro de sintaxe 3 vezes seguidas, parar de tentar adivinhar, retornar mensagem padrão informando falha na consulta analítica e pesquisar via Gemini 3.1 como solucionar o erro.
     - Sempre usar arestas direcionadas e filtrar partições temporais quando aplicável; aplicar `job_labels: {"ca-bq-job": "true"}`.
   - **Regra 3 — Mínimo de 10 Verified Queries Validadas em Pre-Flight:**
     - Antes de publicar o agente, teste fisicamente no BigQuery (`dryRun` `< 500 GB` + execução real com label `ca-bq-job: true` retornando `> 0 rows`) pelo menos **10 Verified Queries** em GQL/`GRAPH_TABLE` sobre `thelook_ecommerce.ecommerce_graph` aplicando as defesas contra bugs conhecidos (`ROW_NUMBER() OVER(...)` para Top-N, `COUNT(DISTINCT)` em joins 1:N e `WHERE col IS NOT NULL`).
   - Ao finalizar a publicação, execute um teste conversacional via `:chat` validando uma pergunta de negócio multi-hop e o guardrail de `0 rows` (ex: vendas em `2099`).
