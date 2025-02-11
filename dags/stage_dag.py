from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

from stage.stage_manager import load_stage_config, process_endpoint
from storage.raw_manager import RawStorageManager

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

def process_stage(endpoint: str, **context):
    """
    Task que:
      1. Busca o arquivo RAW mais recente para o endpoint.
      2. Carrega a configuração Stage.
      3. Processa o endpoint: achata, cria/altera a tabela e insere os registros.
    """
    raw_manager = RawStorageManager()
    raw_file = raw_manager.get_latest_file(endpoint)
    if not raw_file:
        raise Exception(f"Arquivo RAW para {endpoint} não encontrado")
    
    stage_config = load_stage_config()
    result = process_endpoint(endpoint, raw_file, stage_config)
    context['task_instance'].xcom_push(key=f'{endpoint}_stage', value=result)
    return result

with DAG(
    'stage_dag',
    default_args=default_args,
    description='Transforma dados da camada Raw para Stage com flattening dinâmico',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stage', 'transform', 'ecommerce']
) as dag:

    endpoints = ['carts', 'customers', 'logistics', 'products']

    tasks = {}
    for ep in endpoints:
        task = PythonOperator(
            task_id=f'process_{ep}_stage',
            python_callable=process_stage,
            op_kwargs={'endpoint': ep}
        )
        tasks[ep] = task