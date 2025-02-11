from datetime import datetime
import os
import json
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class RawStorageManager:
    """Gerenciador de armazenamento da camada Raw"""
    
    def __init__(self, base_path: str = '../local_storage/raw'):
        """
        Args:
            base_path: Caminho base para armazenamento
        """
        self.base_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            base_path
        )
    
    def save_json(self, data: List[Dict[str, Any]], endpoint: str) -> str:
        """
        Salva dados em JSON seguindo a estrutura:
        local_storage/raw/{endpoint}/{YYYY-MM-DD}/{endpoint}_{YYYYMMDD_HHMMSS}.json
        
        Args:
            data: Lista de registros a serem salvos
            endpoint: Nome do endpoint (products, customers, etc)
            
        Returns:
            Caminho completo do arquivo salvo
        """
        # Gera paths
        date_path = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        full_path = os.path.join(
            self.base_path,
            endpoint,
            date_path
        )
        
        # Cria diretórios
        os.makedirs(full_path, exist_ok=True)
        
        # Define nome do arquivo
        filename = f"{endpoint}_{timestamp}.json"
        filepath = os.path.join(full_path, filename)
        
        # Salva dados
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Dados salvos em: {filepath}")
        return filepath
        
    def get_latest_file(self, endpoint: str) -> str:
        """Retorna o arquivo mais recente de um endpoint"""
        base_path = os.path.join(self.base_path, endpoint)
        
        if not os.path.exists(base_path):
            return None
            
        # Lista todas as datas
        dates = sorted(os.listdir(base_path), reverse=True)
        if not dates:
            return None
            
        # Pega pasta mais recente
        latest_date = dates[0]
        date_path = os.path.join(base_path, latest_date)
        
        # Lista arquivos da data mais recente
        files = sorted(os.listdir(date_path), reverse=True)
        if not files:
            return None
            
        # Retorna arquivo mais recente
        return os.path.join(date_path, files[0])
        
    def list_files(self, endpoint: str, date: str = None) -> List[str]:
        """Lista arquivos de um endpoint/data específicos"""
        pass 