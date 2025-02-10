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
    
    def __init__(self, base_url: str, access_token: str):
        self.base_url = base_url
        self.access_token = access_token
        self.headers = {"Authorization": f"Bearer {access_token}"}
    
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
    def _make_request(self, endpoint: str, skip: int = 0, limit: int = 50) -> Dict:
        """Faz requisição com retry exponencial"""
        url = f"{self.base_url}{endpoint}"
        params = {"skip": skip, "limit": limit}
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500:
                logger.warning(f"Erro 500 detectado em {url}, tentando novamente...")
                raise
            logger.error(f"Erro HTTP {e.response.status_code} em {url}")
            raise
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout na requisição para {url}")
            raise
            
        except requests.exceptions.RequestException as e:
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
            
            data = self._make_request(endpoint, skip, limit)
            
            if not data:  # Página vazia
                break
                
            yield data
            
            if len(data) < limit:  # Última página
                break
                
            skip += limit
