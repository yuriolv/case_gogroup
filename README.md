# 📊 Pipeline de Monitoramento de Preços — Smartphones

## 🧠 Visão Geral

Este projeto implementa um pipeline de dados end-to-end para monitoramento de preços de smartphones em marketplaces públicos.

O objetivo é fornecer inteligência de mercado para apoiar decisões estratégicas de precificação, respondendo perguntas como variação de preços, comportamento de vendedores, impacto de frete grátis e distribuição de preços.

O pipeline cobre desde a **coleta de dados em tempo real até a visualização em dashboard**, utilizando uma arquitetura moderna de engenharia de dados.

---

## 🏗️ Arquitetura do Projeto

A solução foi construída com uma arquitetura baseada em camadas:

```
[ Airflow (Orquestração) ]
        ↓
[ Coleta (Python) ]
        ↓
[ Kafka (Streaming / Buffer) ]
        ↓
[ PostgreSQL (Raw Layer) ]
        ↓
[ dbt (Staging + Marts) ]
        ↓
[ Power BI (Dashboard) ]

```

---

## ⚙️ Stack Utilizada

* **Coleta de dados:** Python (requests + parsing de marketplace)
* **Streaming:** Kafka
* **Banco de dados:** PostgreSQL
* **Transformações:** dbt Core
* **Orquestração:** Apache Airflow
* **Containerização:** Docker + Docker Compose
* **Visualização:** Power BI

---

## 📦 Camadas de Dados

### 🔹 Raw Layer

* Dados ingeridos diretamente do Kafka
* Sem transformação
* Persistência fiel ao dado original da fonte

### 🔹 Staging Layer

* Limpeza e padronização de tipos
* Normalização de campos (preço, booleanos, timestamps)
* Remoção de inconsistências

### 🔹 Mart Layer

* Modelos analíticos finais
* Tabelas fato e dimensão:

  * `fact_product_prices`
  * `dim_sellers`
  * `metrics`
* Otimizado para consumo pelo BI

---

## 🔁 Pipeline End-to-End

O pipeline é totalmente orquestrado via Airflow e executado de forma agendada (hourly).

Etapas:

1. Coleta de dados do marketplace
2. Publicação no Kafka (streaming layer)
3. Consumer grava dados no PostgreSQL (raw)
4. Execução de transformações via dbt
5. Atualização das tabelas analíticas (marts)
6. Consumo via Power BI

---

## 🧠 Principais Decisões Lógicas/Arquiteturais

### 📌 0. Escolha na Fonte dos Dados

O primeiro passo foi definir o marketplace como fonte de dados. Inicialmente, 
o Mercado Livre foi escolhido devido à sua API pública de busca, amplamente conhecida. 
No entanto, por conta de recentes instabilidades, busquei alternativas.

Um dos principais desafios foi a obtenção dos dados, já que a coleta automatizada 
de preços é amplamente restringida pelos marketplaces — seja pelo bloqueio 
de scraping ou pela ausência de APIs públicas. 
Mesmo quando disponíveis, como no caso de APIs de publicidade (ex: Amazon Advertising), 
o acesso costuma ser pago ou exige um processo rigoroso de credenciamento.

Após uma análise mais aprofundada, encontrei a API da Kabum, 
que se mostrou suficiente e adequada para o contexto do case.

---

### 📌 1. Uso de Streaming com Kafka

Foi utilizado Kafka como buffer entre coleta e armazenamento para:

* desacoplar sistemas
* permitir escalabilidade futura
* simular arquitetura real de engenharia de dados

---

### 📌 2. Separação em camadas (Raw / Staging / Mart)

A modelagem segue boas práticas de Data Warehousing:

* rastreabilidade total dos dados
* transformação incremental via dbt
* isolamento de responsabilidade por camada

---

### 📌 3. Idempotência do pipeline (DIFERENCIAL)

Foi implementado controle de duplicidade com base em:

* product_id
* hash de registros

Inicialmente, a estratégia adotada foi utilizar product_id + timestamp da coleta, o que evitava duplicidades a nível de registro. No entanto, do ponto de vista lógico, os dados ainda eram replicados (mesmo conteúdo em diferentes coletas).

Dessa forma, optei por utilizar o hash das principais características do produto + id do produto, garantindo um controle de duplicidade mais eficaz também a nível lógico.

➡️ Isso garante que reprocessamentos não duplicam dados.

---

### 📌 4. Logs estruturados

* Logging implementado no collector e consumer
* Facilita debug em pipelines distribuídos
* Melhor rastreabilidade em produção

---

### 📌 5. Airflow como orquestrador central

* DAGs organizados por etapa do pipeline
* Retry automático em falhas
* Execução agendada (hourly)

---

### 📌 6. dbt para transformação

* Versionamento de SQL
* Separação entre staging e marts
* Testes de qualidade embutidos (nulls, uniqueness, relationships)

---
### 📌 7. Dias de Coleta

Para enriquecer o cenário do case, optei por simular dias distintos de coleta por meio de execuções em horários diferentes. Os intervalos foram definidos de forma a permitir mudanças nos dados retornados pela API, possibilitando a análise de variações ao longo do tempo.

---

## 📊 Dashboard (Power BI)

O dashboard responde às principais perguntas de negócio:

* Preço médio, mínimo e máximo de smartphones
* Evolução do preço ao longo do tempo
* Distribuição de preços (histograma)
* Vendedores com maior volume
* Impacto de frete grátis no preço médio
* Correlação entre desconto e vendas

---

## 🧪 Qualidade de Dados

Foram implementados testes via dbt:

* `not null` em chaves primárias
* `unique` em identificadores de produto
* validação de relacionamentos entre fact e dimensions

---

## 🐳 Execução do Projeto

### 1. Clonar o repositório

```bash
git clone <repo-url>
cd projeto
```

### 2. Subir ambiente completo

```bash
docker compose up -d
```

### 3. Executar pipeline

Airflow será responsável por orquestrar automaticamente.

---

## 📁 Estrutura do Projeto

```
airflow/
dbt/
ingestion (Kafka)/
postgres/
docker-compose.yml
.env.example
README.md
```

---

## 🚀 Diferenciais Implementados

✔ Pipeline end-to-end funcional
✔ Arquitetura streaming (Kafka)
✔ Idempotência no processamento
✔ Modelagem em camadas (raw/stg/marts)
✔ Orquestração com Airflow
✔ Transformações com dbt
✔ Testes de qualidade de dados
✔ Logs estruturados
✔ Dockerização completa

---

## 📌 Possíveis Evoluções

* Incremental models no dbt
* Monitoramento com Prometheus + Grafana
* Particionamento no PostgreSQL
* Deploy em cloud (AWS/GCP)
* CI/CD com GitHub Actions

---

## 👨‍💻 Autor

Projeto desenvolvido como parte de um case técnico de Engenharia de Dados, com foco em arquitetura moderna de pipelines e boas práticas de Data Engineering.
