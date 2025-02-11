import os
import yaml
import json
import logging
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

logger = logging.getLogger(__name__)

def load_stage_config():
    """
    Carrega a configuração dos endpoints para a camada Stage.
    Assume que o arquivo stage_config.yaml está na pasta config no nível raiz (AIRFLOW_HOME/config).
    """
    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    config_path = os.path.join(airflow_home, 'config', 'stage_config.yaml')
    logger.info("Carregando configuração de stage de: %s", config_path)
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def flatten_json(y, parent_key='', sep='_'):
    """
    Achata um dicionário aninhado.
    
    - Se encontrar um dicionário, concatena as chaves com o separador.
    - Se encontrar uma lista:
       * Se todos os itens forem dicionários, une as chaves de todos os itens e as adiciona com o prefixo (sem índice).
       * Se forem valores simples, junta-os em uma única string separada por vírgula.
       * Caso misto ou vazio, armazena a lista como string.
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
    Infere um tipo SQL simples baseado no valor.
    Se a string não iniciar com dígito, força TEXT.
    Se iniciar com dígito, tenta convertê-la via datetime.fromisoformat para TIMESTAMP.
    """
    if value is None:
        return "TEXT"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "DECIMAL(20,6)"
    if isinstance(value, str):
        # Se a string não inicia com dígito, assume TEXT.
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
    Retorna o conjunto de nomes de colunas existentes na tabela.
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
    Retorna um dicionário com o nome da coluna e seu data_type.
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
    Compara o tipo atual (retornado pelo information_schema) com o tipo esperado (inferido).
    Normaliza para comparar:
      - DECIMAL / NUMERIC => "numeric"
      - TIMESTAMP => "timestamp"
      - INTEGER => "integer"
      - TEXT => "text"
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
    Se o tipo atual da coluna não corresponder ao tipo inferido para sample_value,
    executa um ALTER TABLE para atualizar o tipo da coluna.
    """
    expected_type = infer_sql_type(sample_value)
    # Mapeia para um tipo-alvo aceitável no ALTER TABLE
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
    Cria a tabela com as colunas fornecidas.
    `columns` é um dicionário onde a chave é o nome da coluna e o valor é um exemplo para inferir o tipo.
    Se a coluna 'id' estiver presente, ela será definida como PRIMARY KEY.
    """
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
    Adiciona uma coluna à tabela.
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
    Insere os registros na tabela usando ON CONFLICT (id) DO NOTHING para evitar erros de duplicidade.
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

def process_endpoint(endpoint: str, raw_file_path: str, stage_config: dict) -> dict:
    """
    Processa um endpoint:
      1. Lê o arquivo RAW.
      2. Achata cada registro (explodindo JSON e listas).
      3. Cria ou altera a tabela dinamicamente para incluir todas as colunas encontradas,
         atualizando o tipo de coluna se necessário.
      4. Insere os registros (ignorando duplicatas na chave primária).
      
    Retorna um dicionário com informações do processamento.
    """
    with open(raw_file_path, 'r') as f:
        raw_data = json.load(f)
    
    flattened_records = []
    union_keys = set()
    sample_values = {}  # Para inferir os tipos
    
    for rec in raw_data:
        flat_rec = flatten_json(rec)
        flattened_records.append(flat_rec)
        for k, v in flat_rec.items():
            union_keys.add(k)
            if k not in sample_values and v is not None:
                sample_values[k] = v
    
    union_keys = sorted(list(union_keys))
    
    # Obtém o nome da tabela a partir da configuração
    endpoint_config = stage_config.get(endpoint)
    if not endpoint_config:
        raise Exception(f"Stage config para {endpoint} não encontrada")
    table = endpoint_config['table']
    
    # Verifica se a tabela existe; se não, cria-a; se sim, adiciona novas colunas ou atualiza tipos se necessário
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
        columns_with_sample = {k: sample_values.get(k) for k in union_keys}
        create_table(table, columns_with_sample)
    else:
        existing_columns_info = get_existing_columns_info(table)
        for key in union_keys:
            if key not in existing_columns_info:
                alter_table_add_column(table, key, sample_values.get(key))
            else:
                current_type = existing_columns_info[key]
                expected_type = infer_sql_type(sample_values.get(key))
                if not types_match(current_type, expected_type):
                    update_column_type_if_needed(table, key, sample_values.get(key))
    
    # Prepara as linhas para inserção, garantindo a presença de todas as colunas
    rows = []
    for rec in flattened_records:
        row = []
        for key in union_keys:
            row.append(rec.get(key))
        rows.append(tuple(row))
    
    try:
        insert_rows_ignore_conflict(table, rows, union_keys)
        logger.info("Inseridos %d registros na tabela %s", len(rows), table)
    except Exception as e:
        logger.error("Erro ao inserir registros na tabela %s: %s", table, str(e))
        raise
    
    return {"table": table, "records_inserted": len(rows)}