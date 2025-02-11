import os
import yaml
import json
import logging
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

# Configuração do logger para o módulo
logger = logging.getLogger(__name__)

def load_stage_config():
    """
    Carrega a configuração dos endpoints para a camada Stage.
    
    Busca o arquivo stage_config.yaml na pasta config do AIRFLOW_HOME.
    
    Returns:
        dict: Configurações dos endpoints para stage
        
    Raises:
        FileNotFoundError: Se o arquivo de configuração não for encontrado
    """
    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    config_path = os.path.join(airflow_home, 'config', 'stage_config.yaml')
    logger.info("Carregando configuração de stage de: %s", config_path)
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def flatten_json(y, parent_key='', sep='_'):
    """
    Achata um dicionário aninhado em um dicionário plano.
    
    Regras de achatamento:
    1. Dicionários aninhados: concatena as chaves com o separador
    2. Listas:
        - Se todos os itens forem dicionários: une as chaves de todos os itens
        - Se todos os itens forem valores simples: junta em string com vírgulas
        - Caso contrário: converte para string
    
    Args:
        y: Dicionário ou valor a ser achatado
        parent_key: Chave pai para concatenação (default: '')
        sep: Separador usado na concatenação de chaves (default: '_')
        
    Returns:
        dict: Dicionário achatado
    """
    items = {}
    if isinstance(y, dict):
        for k, v in y.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(flatten_json(v, new_key, sep=sep))
            elif isinstance(v, list):
                if v and all(isinstance(i, dict) for i in v):
                    union_dict = {}
                    for item in v:
                        flat_item = flatten_json(item, parent_key="", sep=sep)
                        for sub_key, sub_value in flat_item.items():
                            if sub_key not in union_dict:
                                union_dict[sub_key] = sub_value
                    for sub_key, sub_value in union_dict.items():
                        items[f"{new_key}{sep}{sub_key}"] = sub_value
                elif v and all(not isinstance(i, dict) and not isinstance(i, list) for i in v):
                    items[new_key] = ", ".join(map(str, v))
                else:
                    items[new_key] = str(v)
            else:
                items[new_key] = v
    else:
        items[parent_key] = y
    return items

def infer_sql_type(value):
    """
    Infere o tipo SQL apropriado baseado no valor fornecido.
    
    Regras de inferência:
    1. None -> TEXT
    2. int -> INTEGER
    3. float -> DECIMAL(20,6)
    4. str:
        - Se não começa com dígito -> TEXT
        - Se é data ISO -> TIMESTAMP
        - Se é número inteiro -> INTEGER
        - Se é decimal -> DECIMAL(20,6)
        - Caso contrário -> TEXT
        
    Args:
        value: Valor para inferir o tipo
        
    Returns:
        str: Tipo SQL inferido
    """
    if value is None:
        return "TEXT"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "DECIMAL(20,6)"
    if isinstance(value, str):
        # Se a string não começar com dígito, consideramos TEXT.
        if not value[0].isdigit():
            return "TEXT"
        try:
            datetime.fromisoformat(value)
            return "TIMESTAMP"
        except ValueError:
            try:
                int(value)
                return "INTEGER"
            except:
                try:
                    float(value)
                    return "DECIMAL(20,6)"
                except:
                    return "TEXT"
    return "TEXT"

def get_existing_columns(table, postgres_conn_id='ecommerce'):
    """
    Obtém o conjunto de colunas existentes em uma tabela PostgreSQL.
    
    Args:
        table: Nome da tabela no formato schema.table
        postgres_conn_id: ID da conexão Airflow com PostgreSQL
        
    Returns:
        set: Conjunto com nomes das colunas existentes
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    schema, table_name = table.split('.')
    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
        (schema, table_name)
    )
    return {row[0] for row in cursor.fetchall()}

def get_existing_columns_info(table, postgres_conn_id='ecommerce'):
    """
    Obtém informações sobre as colunas existentes em uma tabela PostgreSQL.
    
    Args:
        table: Nome da tabela no formato schema.table
        postgres_conn_id: ID da conexão Airflow com PostgreSQL
        
    Returns:
        dict: Dicionário com nome da coluna como chave e tipo de dados como valor
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    schema, table_name = table.split('.')
    cursor.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
        (schema, table_name)
    )
    return {row[0]: row[1] for row in cursor.fetchall()}

def types_match(current_type, expected_type):
    """
    Verifica se dois tipos SQL são compatíveis.
    
    Normaliza os tipos para comparação:
    - DECIMAL/Numeric -> numeric
    - TIMESTAMP -> timestamp
    - INTEGER -> integer
    - TEXT -> text
    
    Args:
        current_type: Tipo atual da coluna
        expected_type: Tipo esperado/inferido
        
    Returns:
        bool: True se os tipos são compatíveis, False caso contrário
    """
    expected_type = expected_type.lower()
    if expected_type.startswith("decimal") or expected_type.startswith("numeric"):
        expected_norm = "numeric"
    elif expected_type.startswith("timestamp"):
        expected_norm = "timestamp"
    elif expected_type.startswith("integer"):
        expected_norm = "integer"
    elif expected_type.startswith("text"):
        expected_norm = "text"
    else:
        expected_norm = expected_type

    current_norm = current_type.lower()
    if "numeric" in current_norm and expected_norm == "numeric":
        return True
    if "timestamp" in current_norm and expected_norm == "timestamp":
        return True
    if "integer" in current_norm and expected_norm == "integer":
        return True
    if "text" in current_norm and expected_norm == "text":
        return True
    return False

def update_column_type_if_needed(table, column, sample_value, postgres_conn_id='ecommerce'):
    """
    Atualiza o tipo de uma coluna se necessário.
    
    Args:
        table: Nome da tabela no formato schema.table
        column: Nome da coluna a ser atualizada
        sample_value: Valor de exemplo para inferir o novo tipo
        postgres_conn_id: ID da conexão Airflow com PostgreSQL
    """
    expected_type = infer_sql_type(sample_value)
    if expected_type.upper().startswith("DECIMAL"):
        target_type = "numeric"
    elif expected_type.upper().startswith("TIMESTAMP"):
        target_type = "timestamp"
    elif expected_type.upper().startswith("INTEGER"):
        target_type = "integer"
    elif expected_type.upper().startswith("TEXT"):
        target_type = "text"
    else:
        target_type = expected_type.lower()
    alter_sql = f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {target_type} USING {column}::text;"
    logger.info("Atualizando tipo da coluna %s na tabela %s: %s", column, table, alter_sql)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(alter_sql)
    conn.commit()

def create_table(table, columns, postgres_conn_id='ecommerce'):
    """
    Cria uma nova tabela no PostgreSQL.
    
    Características:
    - Coluna 'id' é definida como PRIMARY KEY se presente
    - Adiciona colunas de controle ETL:
        * etl_load_date (TIMESTAMP)
        * source_file (TEXT)
        * etl_run_id (TEXT)
        
    Args:
        table: Nome da tabela no formato schema.table
        columns: Dicionário com nome e valor exemplo das colunas
        postgres_conn_id: ID da conexão Airflow com PostgreSQL
    """
    # Adiciona colunas de controle se não existirem
    if 'etl_load_date' not in columns:
        columns['etl_load_date'] = datetime.now().isoformat()
    if 'source_file' not in columns:
        columns['source_file'] = 'unknown'
    if 'etl_run_id' not in columns:
        columns['etl_run_id'] = 'unknown'
        
    col_defs = []
    for col, sample_value in columns.items():
        col_type = infer_sql_type(sample_value)
        if col == "id":
            col_defs.append(f"{col} {col_type} PRIMARY KEY")
        else:
            col_defs.append(f"{col} {col_type}")
    create_table_sql = f"CREATE TABLE {table} ({', '.join(col_defs)});"
    logger.info("Criando tabela %s com SQL: %s", table, create_table_sql)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()

def alter_table_add_column(table, column, sample_value, postgres_conn_id='ecommerce'):
    """
    Adiciona uma nova coluna à tabela existente.
    
    Args:
        table: Nome da tabela no formato schema.table
        column: Nome da nova coluna
        sample_value: Valor de exemplo para inferir o tipo
        postgres_conn_id: ID da conexão Airflow com PostgreSQL
    """
    col_type = infer_sql_type(sample_value)
    alter_sql = f"ALTER TABLE {table} ADD COLUMN {column} {col_type};"
    logger.info("Alterando tabela %s: %s", table, alter_sql)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(alter_sql)
    conn.commit()

def insert_rows_ignore_conflict(table, rows, target_fields, postgres_conn_id='ecommerce'):
    """
    Insere registros na tabela ignorando conflitos de chave primária.
    
    Usa ON CONFLICT (id) DO NOTHING para evitar erros de duplicidade.
    
    Args:
        table: Nome da tabela no formato schema.table
        rows: Lista de tuplas com os valores a serem inseridos
        target_fields: Lista com nomes das colunas na ordem dos valores
        postgres_conn_id: ID da conexão Airflow com PostgreSQL
    """
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(target_fields))
    columns = ", ".join(target_fields)
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING;"
    logger.info("Executando inserção com query: %s", sql)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.executemany(sql, rows)
    conn.commit()

def process_endpoint(endpoint: str, raw_file_path: str, stage_config: dict, **context) -> dict:
    """
    Processa um endpoint carregando dados da camada Raw para Stage.
    
    Fluxo de processamento:
    1. Lê arquivo JSON da camada Raw
    2. Achata registros aninhados
    3. Adiciona colunas de controle ETL
    4. Cria/altera tabela conforme necessário
    5. Insere registros ignorando duplicatas
    
    Args:
        endpoint: Nome do endpoint sendo processado
        raw_file_path: Caminho do arquivo JSON na camada Raw
        stage_config: Configurações de stage dos endpoints
        **context: Contexto do Airflow
        
    Returns:
        dict: Estatísticas do processamento (tabela e registros inseridos)
        
    Raises:
        Exception: Se configuração do endpoint não for encontrada
    """
    # Lê dados do arquivo Raw
    with open(raw_file_path, 'r') as f:
        raw_data = json.load(f)
    
    # Prepara estruturas de controle
    flattened_records = []
    union_keys = set()
    sample_values = {}  # Para inferência de tipos
    
    # Dados de controle ETL
    etl_load_date = datetime.now().isoformat()
    source_file = os.path.basename(raw_file_path)
    etl_run_id = context.get("dag_run").run_id if context.get("dag_run") else "unknown"
    
    # Processa cada registro
    for rec in raw_data:
        flat_rec = flatten_json(rec)
        # Adiciona campos de controle
        flat_rec['etl_load_date'] = etl_load_date
        flat_rec['source_file'] = source_file
        flat_rec['etl_run_id'] = etl_run_id
        flattened_records.append(flat_rec)
        for k, v in flat_rec.items():
            union_keys.add(k)
            if k not in sample_values and v is not None:
                sample_values[k] = v
    
    union_keys = sorted(list(union_keys))
    
    # Obtém configuração da tabela
    endpoint_config = stage_config.get(endpoint)
    if not endpoint_config:
        raise Exception(f"Stage config para {endpoint} não encontrada")
    table = endpoint_config['table']
    
    # Verifica/cria/altera tabela
    pg_hook = PostgresHook(postgres_conn_id='ecommerce')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    schema, table_name = table.split('.')
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
        (schema, table_name)
    )
    exists = cursor.fetchone()[0]
    
    if not exists:
        # Cria nova tabela
        columns_with_sample = {k: sample_values.get(k) for k in union_keys}
        create_table(table, columns_with_sample)
    else:
        # Atualiza estrutura existente
        existing_columns_info = get_existing_columns_info(table)
        for key in union_keys:
            if key not in existing_columns_info:
                alter_table_add_column(table, key, sample_values.get(key))
            else:
                current_type = existing_columns_info[key]
                expected_type = infer_sql_type(sample_values.get(key))
                if not types_match(current_type, expected_type):
                    update_column_type_if_needed(table, key, sample_values.get(key))
    
    # Prepara registros para inserção
    rows = []
    for rec in flattened_records:
        row = []
        for key in union_keys:
            row.append(rec.get(key))
        rows.append(tuple(row))
    
    # Insere registros
    try:
        insert_rows_ignore_conflict(table, rows, union_keys)
        logger.info("Inseridos %d registros na tabela %s", len(rows), table)
    except Exception as e:
        logger.error("Erro ao inserir registros na tabela %s: %s", table, str(e))
        raise
    
    return {"table": table, "records_inserted": len(rows)}