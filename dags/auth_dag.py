"""
DAG de Autenticação para API de E-commerce
-----------------------------------------
Responsável pelo gerenciamento de tokens JWT, implementando:
- Autenticação inicial com credenciais
- Refresh automático de tokens antes da expiração
- Tratamento robusto de erros com retry exponencial
- Observabilidade completa com logs detalhados
- Armazenamento seguro em variáveis do Airflow

Fluxo principal:
1. Obtém tokens iniciais via credenciais
2. Armazena tokens nas variáveis do Airflow
3. Monitora validade e renova automaticamente
4. Em caso de falha no refresh, volta para autenticação inicial

Autor: Juscelino Barbosa da Silva Neto
Data: 11/02/2025
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import yaml
import os
import json
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_log,
    after_log,
    retry_if_exception_type
)
from typing import Dict, Any, Optional
from dataclasses import dataclass
from functools import wraps
from api_manager import APIManager

# Configuração do logger para a DAG
logger = logging.getLogger(__name__)

@dataclass
class AuthConfig:
    """
    Classe para validação e armazenamento da configuração de autenticação.
    
    Atributos:
        base_url: URL base da API
        token_endpoint: Endpoint para obtenção de tokens
        refresh_endpoint: Endpoint para renovação de tokens
        username: Usuário para autenticação
        password: Senha para autenticação
        token_expiry_minutes: Tempo de validade do token em minutos
    """
    base_url: str
    token_endpoint: str
    refresh_endpoint: str
    username: str
    password: str
    token_expiry_minutes: int = 30

def log_function_call(func):
    """
    Decorator para logging automático de entrada e saída de funções.
    
    Registra:
    - Início da execução
    - Fim com sucesso
    - Erros ocorridos
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Iniciando execução de {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Finalizada execução de {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Erro em {func.__name__}: {str(e)}")
            raise
    return wrapper

class TokenManager:
    """
    Gerenciador de tokens de autenticação.
    
    Responsável por:
    - Fazer requisições à API com retry
    - Obter tokens iniciais
    - Renovar tokens
    - Tratar erros de forma robusta
    """
    
    def __init__(self, config: AuthConfig):
        """
        Inicializa o gerenciador com as configurações necessárias.
        
        Args:
            config: Instância de AuthConfig com as configurações
        """
        self.config = config
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        before=before_log(logger, logging.INFO),
        after=after_log(logger, logging.INFO),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def make_api_request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Realiza requisições à API com retry exponencial em caso de falhas.
        
        Args:
            method: Método HTTP (GET, POST etc)
            endpoint: Caminho do endpoint
            **kwargs: Argumentos adicionais para a requisição
            
        Returns:
            Dict com a resposta da API
            
        Raises:
            AirflowException: Em caso de falha após todas as tentativas
        """
        url = f"{self.config.base_url}{endpoint}"
        try:
            response = requests.request(method, url, timeout=10, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout na requisição para {url}")
            raise AirflowException("Timeout na requisição à API")
        except requests.exceptions.RequestException as e:
            if hasattr(e.response, 'status_code') and e.response.status_code == 500:
                logger.warning(f"Erro 500 detectado em {url}, tentando novamente...")
                raise
            raise AirflowException(f"Falha na requisição: {str(e)}")

    @log_function_call
    def get_initial_tokens(self) -> Dict[str, str]:
        """
        Obtém par inicial de tokens via credenciais.
        
        Returns:
            Dict contendo access_token e refresh_token
        """
        return self.make_api_request(
            'POST',
            self.config.token_endpoint,
            data={
                "username": self.config.username,
                "password": self.config.password
            }
        )

    @log_function_call
    def refresh_access_token(self, refresh_token: str) -> Dict[str, str]:
        """
        Renova token de acesso usando refresh token.
        
        Args:
            refresh_token: Token de refresh válido
            
        Returns:
            Dict com novos tokens
        """
        return self.make_api_request(
            'POST',
            self.config.refresh_endpoint,
            headers={"Authorization": f"Bearer {refresh_token}"}
        )

def load_config() -> AuthConfig:
    """
    Carrega e valida configurações do arquivo YAML.
    
    Returns:
        Instância de AuthConfig com as configurações validadas
        
    Raises:
        AirflowException: Se houver erro ao carregar/validar configurações
    """
    try:
        config_path = os.path.join(os.path.dirname(__file__), '../config/auth_config.yaml')
        with open(config_path) as f:
            config = yaml.safe_load(f)
            
        return AuthConfig(
            base_url=config['api']['base_url'],
            token_endpoint=config['api']['auth']['token_endpoint'],
            refresh_endpoint=config['api']['auth']['refresh_endpoint'],
            username=config['api']['auth']['username'],
            password=config['api']['auth']['password'],
            token_expiry_minutes=config['api']['auth']['token_expiry_minutes']
        )
    except Exception as e:
        logger.error(f"Erro ao carregar configurações: {str(e)}")
        raise AirflowException(f"Falha ao carregar configuração: {str(e)}")

def get_token(**context) -> Dict[str, str]:
    """
    Task para obtenção inicial de tokens.
    
    Fluxo:
    1. Carrega configurações
    2. Inicializa APIManager
    3. Faz requisição de autenticação
    4. Armazena tokens nas variáveis
    5. Registra status via XCom
    
    Args:
        **context: Contexto do Airflow
        
    Returns:
        Dict com os tokens obtidos
        
    Raises:
        AirflowException: Em caso de falha na autenticação
    """
    config = load_config()
    api_manager = APIManager(config.base_url, None)  # Inicialmente sem token
    
    try:
        # Usa o APIManager para fazer a requisição
        tokens = api_manager.make_request(
            endpoint=config.token_endpoint,
            method='POST',
            data={
                "username": config.username,
                "password": config.password
            }
        )
        
        # Armazena tokens nas variáveis do Airflow
        Variable.set("access_token", tokens['access_token'], serialize_json=True)
        Variable.set("refresh_token", tokens['refresh_token'], serialize_json=True)
        Variable.set("token_timestamp", str(datetime.now()), serialize_json=True)
        
        context['task_instance'].xcom_push(
            key='auth_status',
            value={'success': True, 'timestamp': str(datetime.now())}
        )
        
        return tokens
    except Exception as e:
        context['task_instance'].xcom_push(
            key='auth_status',
            value={'success': False, 'error': str(e)}
        )
        raise

def refresh_token(**context) -> Dict[str, str]:
    """
    Task para renovação de token.
    
    Fluxo:
    1. Verifica validade do token atual
    2. Se necessário, tenta renovar usando refresh token
    3. Em caso de falha, tenta nova autenticação
    4. Atualiza variáveis e registra status
    
    Args:
        **context: Contexto do Airflow
        
    Returns:
        Dict com status da operação ou novos tokens
    """
    config = load_config()
    token_manager = TokenManager(config)
    
    try:
        refresh_token = Variable.get("refresh_token", deserialize_json=True)
        last_token_time = datetime.fromisoformat(
            Variable.get("token_timestamp", deserialize_json=True)
        )
        
        # Verifica validade do token atual
        if datetime.now() - last_token_time < timedelta(minutes=25):
            logger.info("Token ainda válido, pulando renovação")
            return {"status": "valid", "message": "Token ainda válido"}
            
        tokens = token_manager.refresh_access_token(refresh_token)
        
        Variable.set("access_token", tokens['access_token'], serialize_json=True)
        Variable.set("token_timestamp", str(datetime.now()), serialize_json=True)
        
        context['task_instance'].xcom_push(
            key='refresh_status',
            value={'success': True, 'timestamp': str(datetime.now())}
        )
        
        return tokens
    except Exception as e:
        logger.warning(f"Falha no refresh, tentando nova autenticação: {str(e)}")
        return get_token(**context)

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=10)
}

with DAG(
    'api_auth_dag',
    default_args=default_args,
    description='DAG para gerenciamento de autenticação da API',
    schedule_interval=timedelta(minutes=25),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['auth', 'api', 'tokens'],
    doc_md="""
    # DAG de Autenticação da API
    
    ## Objetivo
    Gerenciar tokens de autenticação JWT para acesso à API de E-commerce.
    
    ## Funcionalidades
    * Autenticação inicial com credenciais
    * Renovação automática de tokens
    * Tratamento robusto de erros
    * Retry exponencial para falhas
    * Logging detalhado
    * Rastreamento via XCom
    
    ## Arquitetura
    ```mermaid
    graph TD
        A[Início] --> B[Get Initial Token]
        B --> C[Store in Variables]
        C --> D[Wait 25min]
        D --> E[Refresh Token]
        E --> |Success| D
        E --> |Failure| B
    ```
    
    ## Observabilidade
    * Logs detalhados de cada operação
    * Métricas via XCom
    * Rastreamento de falhas
    
    ## Segurança
    * Tokens armazenados com serialização
    * Renovação automática antes da expiração
    * Fallback para nova autenticação
    
    ## Manutenção
    * Código modular e bem documentado
    * Configuração via YAML
    * Tratamento de erros em camadas
    """
) as dag:

    get_initial_token = PythonOperator(
        task_id='get_initial_token',
        python_callable=get_token,
        doc_md="""
        ### Obtenção Inicial de Tokens
        
        Realiza autenticação inicial e obtém:
        * Access Token
        * Refresh Token
        
        #### Fluxo
        1. Carrega configurações
        2. Faz requisição de autenticação
        3. Armazena tokens nas variáveis
        4. Registra status via XCom
        """,
        retries=5,
        retry_delay=timedelta(seconds=30)
    )

    refresh_access_token = PythonOperator(
        task_id='refresh_access_token',
        python_callable=refresh_token,
        doc_md="""
        ### Renovação de Token
        
        Renova access token antes da expiração:
        * Verifica validade atual
        * Usa refresh token se necessário
        * Atualiza variáveis
        
        #### Fluxo
        1. Verifica timestamp atual
        2. Decide se renova
        3. Atualiza tokens
        4. Registra status
        """,
        retries=5,
        retry_delay=timedelta(seconds=30)
    )

    get_initial_token >> refresh_access_token 