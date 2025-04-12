<<<<<<< HEAD
# projeto_airflow_databricks
=======
# Projeto Airflow + Databricks + Azure Blob

## Como usar

1. Inicie o ambiente:
```bash
docker compose up
```

2. Acesse o Airflow em http://localhost:8080  
Login: admin | Senha: admin

3. Configure a conexão `databricks_default` no Airflow com:
- Host: https://community.cloud.databricks.com
- Token: seu token pessoal do Databricks

4. Altere o DAG (`dag_databricks_blob.py`) com:
- Cluster ID
- Notebook path

5. No notebook (`processar_dados.py`), substitua os dados do Azure Blob (sas token, conta, etc.).

Pronto! Seu Airflow executará notebooks no Databricks Community e integrará com Azure Blob.
>>>>>>> ff55c88 (commit de teste)
