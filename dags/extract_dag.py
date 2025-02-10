from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import yaml
import os
import json
from api_manager import APIManager
import logging

logger = logging.getLogger(__name__)

def load_endpoints_config():
    """Carrega configuração dos endpoints"""
    config_path = os.path.join(os.path.dirname(__file__), '../config/endpoints.yaml')
    with open(config_path) as f:
        return yaml.safe_load(f)

def extract_data(endpoint: str, **context):
    """Extrai dados de um endpoint específico"""
    config = load_endpoints_config()
    resource_config = config['resources'][endpoint]
    
    # Obtém token de acesso
    access_token = Variable.get("access_token", deserialize_json=True)
    
    # Inicializa APIManager
    api_manager = APIManager(
        base_url="http://api:8000",
        access_token=access_token
    )
    
    all_data = []
    
    try:
        # Itera sobre todas as páginas
        for page in api_manager.paginate(
            endpoint=resource_config['endpoint'],
            limit=resource_config['limit']
        ):
            all_data.extend(page)
            logger.info(f"Extraídos {len(page)} registros de {endpoint}")
        
        # Log do total
        logger.info(f"Total de {len(all_data)} registros extraídos de {endpoint}")
        
        # Guarda resultado no XCom
        context['task_instance'].xcom_push(
            key=f'{endpoint}_total_records',
            value=len(all_data)
        )
        
        return all_data
        
    except Exception as e:
        logger.error(f"Erro ao extrair dados de {endpoint}: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    'extract_data_dag',
    default_args=default_args,
    description='Extrai dados da API de E-commerce',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['extract', 'api', 'ecommerce'],
) as dag:
    
    # Cria tasks dinamicamente para cada endpoint
    for endpoint in load_endpoints_config()['resources'].keys():
        extract_task = PythonOperator(
            task_id=f'extract_{endpoint}',
            python_callable=extract_data,
            op_kwargs={'endpoint': endpoint}
        ) 