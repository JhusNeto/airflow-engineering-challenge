from typing import List, Dict, Any, Generator
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_log,
    after_log,
    retry_if_exception_type
)
import logging

logger = logging.getLogger(__name__)

class APIManager:
    """
    Gerenciador de requisições à API com paginação e tratamento de erros.
    
    Esta classe fornece uma interface para fazer requisições HTTP à uma API REST,
    com suporte a:
    - Autenticação via token Bearer
    - Paginação automática dos resultados
    - Retry automático em caso de falhas
    - Logging de operações
    """
    
    def __init__(self, base_url: str, access_token: str = None):
        """
        Inicializa o gerenciador de API.

        Args:
            base_url: URL base da API (ex: https://api.exemplo.com)
            access_token: Token de acesso para autenticação Bearer (opcional)
        """
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {access_token}"} if access_token else {}
    
    def make_request(self, endpoint: str, method: str = 'GET', **kwargs) -> Dict[str, Any]:
        """
        Método público para fazer requisições HTTP.

        Args:
            endpoint: Caminho do endpoint (ex: /users)
            method: Método HTTP (GET, POST, etc)
            **kwargs: Argumentos adicionais passados para requests.request()

        Returns:
            Dict com a resposta da API parseada como JSON
        """
        return self._make_request(endpoint, method=method, **kwargs)
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        before=before_log(logger, logging.INFO),
        after=after_log(logger, logging.WARNING),
        retry=retry_if_exception_type((
            requests.exceptions.RequestException,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError
        ))
    )
    def _make_request(self, endpoint: str, method: str = 'GET', **kwargs) -> Dict:
        """
        Método privado que implementa a lógica de retry nas requisições.
        
        Configurado para:
        - Tentar 5 vezes antes de falhar
        - Espera exponencial entre tentativas (4-60s)
        - Retry apenas para erros de rede/timeout
        
        Args:
            endpoint: Caminho do endpoint
            method: Método HTTP
            **kwargs: Argumentos para requests.request()

        Returns:
            Dict com resposta da API

        Raises:
            requests.exceptions.RequestException: Em caso de erro na requisição
        """
        url = f"{self.base_url}{endpoint}"
        
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers'].update(self.headers)
        
        # Tratamento especial para autenticação via form-data
        if endpoint == '/token' and method == 'POST':
            data = kwargs.pop('data', {})
            form_data = {
                'username': data.get('username'),
                'password': data.get('password'),
                'grant_type': 'password'
            }
            response = requests.post(url, data=form_data, timeout=10)
            response.raise_for_status()
            return response.json()
        
        try:
            response = requests.request(method, url, timeout=10, **kwargs)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Erro na requisição para {url}: {str(e)}")
            raise

    def paginate(self, endpoint: str, limit: int = 50) -> Generator[List[Dict], None, None]:
        """
        Gerador que itera sobre todas as páginas de dados de um endpoint.
        Implementação mais flexível com suporte a limite configurável.
        
        Args:
            endpoint: Caminho do endpoint
            limit: Limite de registros por página (max 50)
            
        Yields:
            Lista de registros de cada página
        """
        skip = 0
        
        while True:
            logger.info(f"Buscando dados de {endpoint} (skip={skip}, limit={limit})")
            
            data = self.make_request(
                endpoint=endpoint,
                method='GET',
                params={'skip': skip, 'limit': limit}
            )
            
            if not data:  # Página vazia
                break
                
            yield data
            
            if len(data) < limit:  # Última página
                break
                
            skip += limit
