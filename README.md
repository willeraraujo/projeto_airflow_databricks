# 🚀 Data Pipeline com Airflow + Databricks + Azure Storage

Este repositório contém uma arquitetura moderna de pipeline de dados orquestrada com **Apache Airflow** (Astronomer project), integrada ao **Azure Databricks** e **Azure Storage**. A pipeline executa notebooks em três camadas: **Bronze**, **Silver** e **Gold**, com as camadas silver e gold com controle de catálogo pelo **Unity Catalog**.

<p align="center">
  <img src="./airflow.png" alt="Arquitetura do pipeline" width="600"/>
</p>
---

## 📁 Estrutura do Projeto

. ├── dags/ # DAGs do Airflow │ └── pipeline_datalake.py ├── docker/ # Configuração do ambiente Docker │ └── Dockerfile ├── include/ │ └── notebooks/ # Notebooks Databricks │ ├── bronze/ │ │ └── ingest_json_data.py │ ├── silver/ │ │ └── process_to_silver.py │ └── gold/ │ └── aggregate_gold_metrics.py ├── plugins/ # Plugins do Airflow (opcional) ├── config/ # Configs e templates auxiliares ├── .env # Variáveis de ambiente sensíveis ├── requirements.txt # Dependências do Airflow └──



## Sobre a arquitetura
🔸 Bronze Layer

Ingestão de dados crus vindos da API https://api.openbrewerydb.org .

Escrita em arquivos .json no Azure Data Lake.

🔹 Silver Layer

Estruturação dos dados brutos e particionamento por localidade

Escrita em tabelas Delta no Unity Catalog.

🟡 Gold Layer

Agregação e dataset pronto para consumo analítico.

Tabelas Delta gerenciadas via Unity Catalog.
