from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from pendulum import datetime
from datetime import timedelta
import time

# Configurações do cluster (igual ao que você forneceu)
CLUSTER_CONFIG = {
    "cluster_name": "cluster_pipe",
    "spark_version": "15.4.x-scala2.12",
    "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*, 4]"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_F4s",
    "driver_node_type_id": "Standard_F4s",
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "autotermination_minutes": 20,
    "enable_elastic_disk": True,
    "single_user_name": "willerags@gmail.com",
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "STANDARD",
    "num_workers": 0
}

# Configurações dos notebooks
NOTEBOOK_PATHS = {
    "bronze": "notebooks/notebook/bronze",
    "silver": "notebooks/notebook/silver",
    "gold": "notebooks/notebook/gold"
}

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    tags=['databricks', 'etl']
)
def databricks_etl_pipeline():
    
    @task
    def create_and_wait_for_cluster():
        """Cria o cluster e espera até estar pronto"""
        hook = DatabricksHook()
        
        # Verifica se o cluster já existe
        existing = hook._do_api_call(('GET', 'api/2.0/clusters/list'), {})
        for cluster in existing.get('clusters', []):
            if cluster['cluster_name'] == CLUSTER_CONFIG['cluster_name']:
                cluster_id = cluster['cluster_id']
                print(f"Usando cluster existente: {cluster_id}")
                break
        else:
            # Cria novo cluster se não existir
            response = hook._do_api_call(
                ('POST', 'api/2.0/clusters/create'),
                CLUSTER_CONFIG
            )
            cluster_id = response['cluster_id']
            print(f"Cluster criado com ID: {cluster_id}")
        
        # Espera o cluster ficar pronto
        while True:
            status = hook._do_api_call(
                ('GET', 'api/2.0/clusters/get'),
                {'cluster_id': cluster_id}
            )
            if status['state'] == 'RUNNING':
                return cluster_id
            elif status['state'] in ['TERMINATED', 'ERROR']:
                raise Exception(f"Cluster falhou: {status['state']}")
            print(f"Cluster status: {status['state']} - Aguardando...")
            time.sleep(30)
    
    # Tarefa para preparar o cluster
    cluster_id = create_and_wait_for_cluster()
    
    # Função auxiliar para criar tarefas de notebook
    def create_notebook_task(task_id, notebook_path):
        return DatabricksSubmitRunOperator(
            task_id=task_id,
            databricks_conn_id="databricks_default",
            existing_cluster_id=cluster_id,
            notebook_task={
                "notebook_path": f"/Users/willerags@gmail.com/{notebook_path}",
                "source": "WORKSPACE"
            }
        )
    
    # Cria as tarefas de notebook
    bronze = create_notebook_task("bronze", NOTEBOOK_PATHS["bronze"])
    silver = create_notebook_task("silver", NOTEBOOK_PATHS["silver"])
    gold = create_notebook_task("gold", NOTEBOOK_PATHS["gold"])
    
    # Define o fluxo de execução
    cluster_id >> bronze >> silver >> gold

# Instancia a DAG
dag = databricks_etl_pipeline()