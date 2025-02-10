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
    """Gerenciador de requisições à API com paginação e tratamento de erros"""
    
    def __init__(self, base_url: str, access_token: str = None):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {access_token}"} if access_token else {}
    
    def make_request(self, endpoint: str, method: str = 'GET', **kwargs) -> Dict[str, Any]:
        """Método público para fazer requisições"""
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
        """Método privado com implementação do retry"""
        url = f"{self.base_url}{endpoint}"
        
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers'].update(self.headers)
        
        # Tratamento especial para autenticação
        if endpoint == '/token' and method == 'POST':
            data = kwargs.pop('data', {})
            # Converte para form-data
            form_data = {
                'username': data.get('username'),
                'password': data.get('password'),
                'grant_type': 'password'
            }
            # Usa requests.post diretamente para form-data
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

    def get_paginated_data(self, endpoint: str) -> Generator[List[Dict], None, None]:
        """
        Gerador que itera sobre todas as páginas de dados
        
        Args:
            endpoint: Endpoint da API
            
        Yields:
            Lista de registros de cada página
        """
        skip = 0
        limit = 50
        
        while True:
            logger.info(f"Buscando dados de {endpoint} (skip={skip}, limit={limit})")
            
            data = self._make_request(endpoint, skip=skip, limit=limit)
            
            if not data:  # Página vazia
                break
                
            yield data
            
            if len(data) < limit:  # Última página
                break
                
            skip += limit

    def paginate(self, endpoint: str, limit: int = 50) -> Generator[List[Dict], None, None]:
        """
        Gerador que itera sobre todas as páginas de dados
        
        Args:
            endpoint: Endpoint da API
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
