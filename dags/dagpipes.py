import logging
from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from pendulum import datetime
from datetime import timedelta
import time
from airflow.utils.trigger_rule import TriggerRule

# Configuration
USER_EMAIL = "willerags@gmail.com"
CLUSTER_NAME = "cluster_pipes"

# Cluster configuration 
CLUSTER_CONFIG = {
    "cluster_name": CLUSTER_NAME,
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
    "single_user_name": USER_EMAIL,
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "STANDARD",
    "num_workers": 0
}

# Notebook paths
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
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email': USER_EMAIL
    },
    tags=['databricks', 'etl']
)
def etl_pipeline():
    
    @task
    def create_and_wait_for_cluster():
        """Create and wait for the cluster to be ready"""
        hook = DatabricksHook()
        
        # Check if the cluster already exists
        existing_clusters = hook._do_api_call(('GET', 'api/2.0/clusters/list'), {})
        for cluster in existing_clusters.get('clusters', []):
            if cluster['cluster_name'] == CLUSTER_NAME:
                if cluster['state'] == 'TERMINATED':
                    # Restart if it is terminated
                    hook._do_api_call(
                        ('POST', 'api/2.0/clusters/start'),
                        {'cluster_id': cluster['cluster_id']}
                    )
                return cluster['cluster_id']
        
        # Create a new cluster if it doesn't exist
        response = hook._do_api_call(
            ('POST', 'api/2.0/clusters/create'),
            CLUSTER_CONFIG
        )
        cluster_id = response['cluster_id']
        
        # Wait until the cluster is running
        while True:
            status = hook._do_api_call(
                ('GET', 'api/2.0/clusters/get'),
                {'cluster_id': cluster_id}
            )
            state = status['state']
            if state == 'RUNNING':
                logging.info(f"Cluster {CLUSTER_NAME} is running.")
                return cluster_id
            elif state in ['TERMINATED', 'ERROR', 'UNKNOWN']:
                raise Exception(f"Cluster failed with state: {state}")
            logging.info(f"Cluster status: {state} - Waiting...")
            time.sleep(30)
    
    @task(
        trigger_rule=TriggerRule.ALL_DONE,
        retries=3,
        retry_delay=timedelta(minutes=2)
    )
    def safe_delete_cluster(cluster_id):
        """Safely delete the cluster after all tasks are completed"""
        hook = DatabricksHook()
        
        try:
            # Check current status
            status = hook._do_api_call(
                ('GET', 'api/2.0/clusters/get'),
                {'cluster_id': cluster_id}
            )
            
            if status['state'] == 'RUNNING':
                logging.info(f"Starting safe deletion of cluster {cluster_id}")
                hook._do_api_call(
                    ('POST', 'api/2.0/clusters/delete'),
                    {'cluster_id': cluster_id}
                )
                
                # Optional termination check
                for _ in range(6):  # 3 minutes timeout
                    try:
                        status = hook._do_api_call(
                            ('GET', 'api/2.0/clusters/get'),
                            {'cluster_id': cluster_id}
                        )
                        if status['state'] == 'TERMINATED':
                            logging.info(f"Cluster {cluster_id} terminated successfully.")
                            return
                        time.sleep(30)
                    except Exception as e:
                        if "does not exist" in str(e):
                            logging.info(f"Cluster {cluster_id} removed successfully.")
                            return
                        raise
                logging.warning("Warning: Timeout during termination check")
                
            elif status['state'] in ['TERMINATING', 'TERMINATED']:
                logging.info(f"Cluster already in state: {status['state']}")
            else:
                logging.warning(f"Cluster in unexpected state: {status['state']}")
                
        except Exception as e:
            if "does not exist" in str(e):
                logging.info("Cluster no longer exists")
            else:
                logging.error(f"Error during deletion: {str(e)}")
                raise
    
    # ===== MAIN TASKS =====
    cluster_id = create_and_wait_for_cluster()
    
    # Notebook tasks using the same cluster
    bronze_task = DatabricksSubmitRunOperator(
        task_id="bronze_layer",
        databricks_conn_id="databricks_default",
        existing_cluster_id=cluster_id,
        notebook_task={
            "notebook_path": f"/Users/{USER_EMAIL}/{NOTEBOOK_PATHS['bronze']}",
            "source": "WORKSPACE"
        }
    )
    
    silver_task = DatabricksSubmitRunOperator(
        task_id="silver_layer",
        databricks_conn_id="databricks_default",
        existing_cluster_id=cluster_id,
        notebook_task={
            "notebook_path": f"/Users/{USER_EMAIL}/{NOTEBOOK_PATHS['silver']}",
            "source": "WORKSPACE"
        }
    )
    
    gold_task = DatabricksSubmitRunOperator(
        task_id="gold_layer",
        databricks_conn_id="databricks_default",
        existing_cluster_id=cluster_id,
        notebook_task={
            "notebook_path": f"/Users/{USER_EMAIL}/{NOTEBOOK_PATHS['gold']}",
            "source": "WORKSPACE"
        }
    )
    
    # ===== TASK EXECUTION ORDER =====
   
    cluster_id >> bronze_task >> silver_task >> gold_task >> safe_delete_cluster(cluster_id)

# Instantiate the DAG
etl_pipeline_dag = etl_pipeline()
