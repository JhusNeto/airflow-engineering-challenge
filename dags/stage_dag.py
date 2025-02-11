from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging

from stage.stage_manager import load_stage_config, process_endpoint
from storage.raw_manager import RawStorageManager

# Configuração do logger para o módulo
logger = logging.getLogger(__name__)

# Argumentos padrão para todas as tasks da DAG
default_args = {
    'owner': 'airflow',  # Proprietário da DAG
    'depends_on_past': False,  # Não depende de execuções anteriores
    'email_on_failure': False,  # Não envia email em caso de falha
    'email_on_retry': False,  # Não envia email em caso de retry
    'retries': 3,  # Número de tentativas em caso de falha
    'retry_delay': timedelta(seconds=30)  # Intervalo entre tentativas
}

def process_stage(endpoint: str, **context):
    """
    Processa um endpoint específico da camada Raw para Stage.
    
    Fluxo de execução:
    1. Busca o arquivo mais recente do endpoint na camada Raw
    2. Carrega as configurações de transformação da camada Stage
    3. Processa os dados aplicando:
        - Flattening de estruturas aninhadas
        - Criação/alteração dinâmica de tabelas
        - Adição de colunas de controle
        - Inserção dos registros processados
    
    Args:
        endpoint (str): Nome do endpoint a ser processado (ex: products, customers)
        context: Contexto da execução do Airflow (injetado automaticamente)
    
    Returns:
        dict: Resultado do processamento contendo:
            - table: Nome da tabela onde os dados foram inseridos
            - records_inserted: Quantidade de registros inseridos
            
    Raises:
        Exception: Se não encontrar arquivo Raw para o endpoint
    """
    # Inicializa gerenciador da camada Raw
    raw_manager = RawStorageManager()
    
    # Busca arquivo mais recente do endpoint
    raw_file = raw_manager.get_latest_file(endpoint)
    if not raw_file:
        raise Exception(f"Arquivo RAW para {endpoint} não encontrado")
    
    # Carrega configurações e processa o endpoint
    stage_config = load_stage_config()
    result = process_endpoint(endpoint, raw_file, stage_config, **context)
    
    # Armazena resultado no XCom para comunicação entre tasks
    context['task_instance'].xcom_push(key=f'{endpoint}_stage', value=result)
    return result

# Definição da DAG
with DAG(
    'stage_dag',
    default_args=default_args,
    description='Transforma dados da camada Raw para Stage com flattening dinâmico e colunas de controle',
    schedule_interval='@hourly',  # Executa a cada 1 hora
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Não executa DAGs passadas
    tags=['stage', 'transform', 'ecommerce']
) as dag:

    # Sensor que aguarda a conclusão da DAG de extração
    wait_for_extract = ExternalTaskSensor(
        task_id='wait_for_extract_dag',
        external_dag_id='extract_data_dag',
        external_task_id=None,  # None significa aguardar a DAG inteira
        timeout=600,  # timeout de 10 minutos
        poke_interval=30,  # verifica a cada 30 segundos
        mode='poke'  # modo de verificação contínua
    )

    # Lista de endpoints a serem processados
    endpoints = ['carts', 'customers', 'logistics', 'products']

    # Cria tasks de processamento para cada endpoint
    tasks = {}
    for ep in endpoints:
        task = PythonOperator(
            task_id=f'process_{ep}_stage',
            python_callable=process_stage,
            op_kwargs={'endpoint': ep},
            provide_context=True  # Injeta o contexto do Airflow
        )
        tasks[ep] = task
        
        # Define a dependência: aguarda extração antes de processar
        wait_for_extract >> task