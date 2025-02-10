from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import yaml
import os
import json
from api_manager import APIManager
import logging
from storage.raw_manager import RawStorageManager

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
        # Extrai dados
        for page in api_manager.paginate(
            endpoint=resource_config['endpoint'],
            limit=resource_config['limit']
        ):
            all_data.extend(page)
            logger.info(f"Extraídos {len(page)} registros de {endpoint}")
        
        # Salva na camada Raw
        raw_manager = RawStorageManager()
        raw_filepath = raw_manager.save_json(all_data, endpoint)
        
        # XCom metadata
        context['task_instance'].xcom_push(
            key=f'{endpoint}_metadata',
            value={
                'total_records': len(all_data),
                'raw_filepath': raw_filepath,
                'timestamp': datetime.now().isoformat()
            }
        )
        
        return all_data
        
    except Exception as e:
        logger.error(f"Erro ao extrair dados de {endpoint}: {str(e)}")
        raise

def save_raw_data(data: list, endpoint: str, **context) -> str:
    """
    Salva dados brutos em JSON na camada Raw
    
    Args:
        data: Lista de registros a serem salvos
        endpoint: Nome do endpoint (products, carts, etc)
        
    Returns:
        Caminho do arquivo salvo
    """
    # Cria estrutura de diretórios
    date_path = datetime.now().strftime('%Y-%m-%d')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    base_path = os.path.join(
        os.path.dirname(__file__), 
        '../local_storage/raw',
        endpoint,
        date_path
    )
    
    os.makedirs(base_path, exist_ok=True)
    
    # Define nome do arquivo
    filename = f"{endpoint}_{timestamp}.json"
    filepath = os.path.join(base_path, filename)
    
    # Salva dados
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    logger.info(f"Dados salvos em: {filepath}")
    return filepath

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