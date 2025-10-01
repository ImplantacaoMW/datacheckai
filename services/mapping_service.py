import re
import psycopg2
from thefuzz import fuzz

def normalizar_nome(nome):
    """Normaliza um nome de coluna, removendo caracteres especiais e convertendo para minúsculas."""
    return re.sub(r'[^a-z0-9]', '', str(nome).lower())

def similaridade(a, b):
    """Calcula a similaridade entre duas strings usando a biblioteca thefuzz."""
    return fuzz.ratio(a, b) / 100.0

def load_mapping_history(db_conn, tipo_layout):
    """
    Carrega as amostras da tabela de layout específica e as agrupa por campo.
    Utiliza uma conexão de banco de dados (`db_conn`) passada como argumento.
    """
    if db_conn is None:
        return {}

    table_name = tipo_layout
    history_data = {}
    query = f"""
        SELECT field_name, array_agg(sample_value)
        FROM {table_name}
        GROUP BY field_name;
    """

    try:
        with db_conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
            for row in results:
                field_name, samples = row
                history_data[field_name] = {"amostras_validas": samples or []}
        return history_data
    except psycopg2.errors.UndefinedTable:
        # A tabela pode não existir ainda, o que é normal na primeira execução.
        db_conn.rollback()
        return {}
    except Exception as e:
        print(f"Erro ao carregar histórico de '{table_name}': {e}")
        return {}

def save_mapping_history(db_conn, tipo_layout, novas_amostras_data):
    """
    Insere novas amostras na tabela de layout apropriada, uma por uma.
    Utiliza uma conexão de banco de dados (`db_conn`) passada como argumento.
    """
    if db_conn is None or not novas_amostras_data:
        return

    table_name = tipo_layout
    query = f"""
        INSERT INTO {table_name} (field_name, sample_value)
        VALUES (%s, %s)
        ON CONFLICT (field_name, sample_value) DO NOTHING;
    """

    try:
        with db_conn.cursor() as cur:
            for field, data in novas_amostras_data.items():
                for sample in data.get("amostras_validas", []):
                    cur.execute(query, (field, sample))
        db_conn.commit()
    except Exception as e:
        print(f"Erro ao salvar amostra em '{table_name}': {e}")
        db_conn.rollback()