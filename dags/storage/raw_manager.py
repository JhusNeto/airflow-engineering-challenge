from datetime import datetime
import os
import json
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class RawStorageManager:
    """
    Gerenciador de armazenamento da camada Raw do data lake.
    
    Responsável por:
    - Salvar dados brutos em formato JSON
    - Organizar arquivos por endpoint e data
    - Recuperar arquivos mais recentes
    """
    
    def __init__(self, base_path: str = '../local_storage/raw'):
        """
        Inicializa o gerenciador com o caminho base de armazenamento.

        Args:
            base_path: Caminho base onde os dados serão armazenados.
                      Padrão: '../local_storage/raw'
        """
        self.base_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            base_path
        )
    
    def save_json(self, data: List[Dict[str, Any]], endpoint: str) -> str:
        """
        Salva dados em formato JSON na camada Raw, organizando por endpoint e data.
        
        Estrutura de diretórios:
        local_storage/
            raw/
                {endpoint}/
                    {YYYY-MM-DD}/
                        {endpoint}_{YYYYMMDD_HHMMSS}.json
        
        Args:
            data: Lista de dicionários contendo os registros a serem salvos
            endpoint: Nome do endpoint (ex: products, customers, etc)
            
        Returns:
            str: Caminho completo do arquivo JSON salvo
        """
        # Gera paths com data atual e timestamp
        date_path = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Constrói caminho completo do diretório
        full_path = os.path.join(
            self.base_path,
            endpoint,
            date_path
        )
        
        # Cria estrutura de diretórios se não existir
        os.makedirs(full_path, exist_ok=True)
        
        # Define nome do arquivo com timestamp
        filename = f"{endpoint}_{timestamp}.json"
        filepath = os.path.join(full_path, filename)
        
        # Salva dados em JSON formatado
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Dados salvos em: {filepath}")
        return filepath
        
    def get_latest_file(self, endpoint: str) -> str:
        """
        Retorna o arquivo mais recente de um determinado endpoint.
        
        Args:
            endpoint: Nome do endpoint para busca
            
        Returns:
            str: Caminho do arquivo mais recente ou None se não encontrado
        """
        base_path = os.path.join(self.base_path, endpoint)
        
        # Verifica se existe diretório para o endpoint
        if not os.path.exists(base_path):
            return None
            
        # Lista e ordena pastas de data (mais recente primeiro)
        dates = sorted(os.listdir(base_path), reverse=True)
        if not dates:
            return None
            
        # Obtém pasta da data mais recente
        latest_date = dates[0]
        date_path = os.path.join(base_path, latest_date)
        
        # Lista e ordena arquivos da data mais recente
        files = sorted(os.listdir(date_path), reverse=True)
        if not files:
            return None
            
        # Retorna caminho do arquivo mais recente
        return os.path.join(date_path, files[0])