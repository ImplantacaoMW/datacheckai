from flask import Flask, render_template, request, redirect, url_for, session, jsonify, abort, g
from flask_session import Session
import psycopg2
from psycopg2.extras import Json
import pandas as pd
import io
import os
import re
import secrets
import json
import tempfile
import shutil
import csv
import math
import datetime
import unicodedata
import glob
from werkzeug.utils import secure_filename
from thefuzz import fuzz
from decimal import Decimal

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
app.secret_key = secrets.token_hex(16)
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = os.path.join(app.root_path, 'flask_session')



app.config['SESSION_PERMANENT'] = False
Session(app)

# Configuração do Banco de Dados
app.config['DB_HOST'] = os.environ.get('DB_HOST', 'localhost')
app.config['DB_NAME'] = os.environ.get('DB_NAME', 'datacheck')
app.config['DB_USER'] = os.environ.get('DB_USER', 'postgres')
app.config['DB_PASS'] = os.environ.get('DB_PASS', 'xbala')

def get_db():
    """Abre uma nova conexão com o banco de dados se não houver uma no contexto da requisição."""
    if 'db' not in g:
        try:
            g.db = psycopg2.connect(
                host=app.config['DB_HOST'],
                database=app.config['DB_NAME'],
                user=app.config['DB_USER'],
                password=app.config['DB_PASS']
            )
        except psycopg2.OperationalError:
            g.db = None
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    """Fecha a conexão com o banco de dados ao final da requisição."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    """Cria uma tabela separada para cada layout para armazenar amostras aprendidas."""
    conn = get_db()
    if conn is None:
        print("ERRO: Não foi possível conectar ao banco de dados PostgreSQL.")
        return

    try:
        with conn.cursor() as cur:
            # Remove tabelas de implementações anteriores para garantir um estado limpo
            cur.execute("DROP TABLE IF EXISTS learned_samples;")
            for layout_name in LAYOUTS.keys():
                cur.execute(f"DROP TABLE IF EXISTS history_{layout_name};")
            cur.execute("DROP TABLE IF EXISTS mapping_history;")

            # Cria uma tabela para cada layout
            for layout_name in LAYOUTS.keys():
                # Remove "s" do final para nomes como "mercadorias" -> "mercadoria"
                # table_name = layout_name.rstrip('s') if layout_name.endswith('s') else layout_name
                table_name = layout_name
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id SERIAL PRIMARY KEY,
                        field_name VARCHAR(255) NOT NULL,
                        sample_value TEXT NOT NULL,
                        CONSTRAINT unique_sample_in_{table_name} UNIQUE (field_name, sample_value)
                    );
                """)
        conn.commit()
    except Exception as e:
        print(f"Erro ao inicializar o banco de dados: {e}")
        conn.rollback()

# LAYOUTS and other constants...
LAYOUT_MERCADORIA = [
    ('codigo', 'Código *', 'Texto', 20, True),
    ('nome', 'Descrição *', 'Texto', 50, True),
    ('unidade', 'Unidade *', 'Texto', 2, True),
    ('marca', 'Marca *', 'Texto', 30, True),
    ('tipo', 'Tipo de Mercadoria *', 'Texto', 20, True),
    ('ncm', 'NCM *', 'Texto', 10, True),
    ('tributacao', 'Tributação *', 'Texto', 50, True),
    ('preco_venda', 'Preço Venda *', 'Numérico', '', True),
    ('preco_custo_aquisicao', 'Preço Custo Aquisição *', 'Numérico', '', True),
    ('original', 'Original', 'Booleano', '', False),
    ('aplicacao', 'Aplicação', 'Texto', 50, False),
    ('origem', 'Origem', 'Numérico', '', False),
    ('anp', 'ANP', 'Numérico', '', False),
    ('coeficiente', 'Coeficiente', 'Texto', 30, False),
    ('qtd_embalagem', 'Quantidade Embalagem', 'Numérico', '', False),
    ('curva_abc', 'Curva ABC', 'Texto', 1, False),
    ('curva_xyz', 'Curva XYZ', 'Texto', 1, False),
    ('cod_original', 'Código Original', 'Texto', 20, False),
    ('cest', 'CEST', 'Texto', 7, False),
    ('preco_venda_sugerido', 'Preço Venda Sugerido', 'Numérico', '', False),
    ('preco_garantia', 'Preço Garantia', 'Numérico', '', False),
    ('preco_custo_fabrica', 'Preço Custo Fábrica', 'Numérico', '', False),
]
LAYOUT_MERCADORIA_SALDOS = [
    ('codigo', 'Código *', 'Texto', 20, True),
    ('tipo_localizacao', 'Tipo de Localização', 'Texto', 50, False),
    ('localizacao', 'Localização', 'Texto', 50, False),
    ('custo_medio', 'Custo Médio *', 'Numérico', 17, True),
    ('custo_medio_contabil', 'Custo Médio Contábil *', 'Numérico', 17, True),
    ('custo_ultima_compra', 'Custo Última Compra *', 'Numérico', 17, True),
    ('base_media_icms_st', 'Base Média ICMS ST', 'Numérico', 17, False),
    ('valor_medio_icms_st', 'Valor Médio ICMS ST', 'Numérico', 17, False),
    ('saldo', 'Saldo *', 'Numérico', 14, True),
    ('custo_contabil_ultima_compra', 'Custo Contábil Última Compra *', 'Numérico', 17, True),
]

LAYOUT_PESSOAS = [
    # (campo, label, tipo, tamanho, obrigatorio)
    ('cpf_cnpj', 'CPF / CNPJ *', 'Texto', 14, True), # PF=11, PJ=14 / (ambos alfanuméricos)
    ('nome_razao', 'NOME / RAZÃO *', 'Texto', 100, True),
    ('apelido_fantasia', 'APELIDO / FANTASIA', 'Texto', 100, False),
    ('rg', 'RG', 'Texto', 17, False),
    ('uf_rg', 'UF RG', 'Numérico', 2, False),
    ('inscricao_municipal', 'INSCRIÇÃO MUNICIPAL', 'Numérico', 50, False),
    ('tipo_pessoa', 'TIPO PESSOA *', 'Numérico', 1, True), #1, 2 (Física / Jurídica)
    ('tipo_contribuinte', 'TIPO CONTRIBUINTE *', 'Numérico', 1, True), # 1, 2 ou 9 (Contribuinte Icms/Contribuinte Isento de IE/Não Contribuinte)
    ('sexo', 'SEXO', 'Numérico', 1, False), #1, 2 (Feminino / Masculino)
    ('estado_civil', 'ESTADO CIVIL', 'Numérico', 1, False), #1 a 7 (Casado(a), Solteiro(a), Separado(a), Viúvo(a), Desquitado(a), Divorciado(a), Outros)
    ('nacionalidade', 'NACIONALIDADE *', 'Numérico', 2, False),
    ('data_nascimento', 'DATA DE NASCIMENTO', 'Data', 10, False),
    ('data_emancipacao', 'DATA EMANCIPAÇÃO', 'Data', 10, False),
    ('tipo_endereco', 'TIPO DE ENDEREÇO *', 'Numérico', 1, True),  # 1 a 6 (Residencial, Comercial, Cobrança, Secundário, Entrega, Coleta)
    ('cep', 'CEP *', 'Texto', 10, True),
    ('logradouro', 'LOGRADOURO *', 'Texto', 50, True),
    ('numero_endereco', 'NÚMERO ENDEREÇO *', 'Texto', 10, True),
    ('bairro', 'BAIRRO *', 'Texto', 50, True),
    ('municipio', 'MUNICÍPIO *', 'Texto', 50, True),
    ('uf', 'UF *', 'Texto', 2, True),
    ('complemento_endereco', 'COMPLEMENTO ENDEREÇO', 'Texto', 20, False),
    ('tipo_telefone', 'TIPO DE TELEFONE *', 'Numérico', 1, True),  # 1 a 6 (Celular Comercial, Residencial, Fax Comercial, Fax Residencial, Nextel)
    ('ddi_telefone', 'DDI  TELEFONE *', 'Texto', 3, False),
    ('ddd_telefone', 'DDD TELEFONE *', 'Texto', 2, True),
    ('telefone', 'TELEFONE *', 'Texto', 20, True),
    ('ramal', 'RAMAL ', 'Texto', 10, False),
    ('contato', 'CONTATO *', 'Texto', 20, False), #1 a 9 (E-mail, Blog, Site, Facebook, Twitter, Skype, Linkedin, Pinterest, Instagram)
    ('email', 'E-MAIL ', 'Texto', 100, False),
    ('numero_produtor_rural', 'Nº PRODUTOR RURAL', 'Texto', 20, False),
    ('data_limite_credito', 'DATA LIMITE DE CRÉDITO', 'Data', 10, False),
    ('valor_limite_credito', 'VALOR LIMITE DE CRÉDITO', 'Numérico', 17, False),
    ('finalidade_contato', 'FINALIDADE DO CONTATO', 'Texto', 50, False),
    ('ie', 'IE', 'Texto', 17, False),
    ('uf_ie', 'UF IE', 'Numérico', 2, False),
    ('produtor_rural', 'PRODUTOR RURAL', 'Booleano', 1, False),
]

LAYOUT_VEICULO_CLIENTE = [
    ('cpf_cnpj', 'CPF/CNPJ *', 'Numérico', 14, True),
    ('placa', 'Placa', 'Texto', 8, False),
    ('modelo', 'Modelo *', 'Texto', 50, True),
    ('cor', 'Cor *', 'Texto', 50, True),
    ('ano_fabricacao', 'Ano Fabricação *', 'Numérico', 4, True),
    ('ano_modelo', 'Ano Modelo *', 'Numérico', 4, True),
    ('chassi', 'Chassi *', 'Texto', 20, True),
    ('motor', 'Motor', 'Texto', 25, False),
    ('renavam', 'Renavam', 'Texto', 11, False),
    ('crlv', 'CRLV', 'Texto', 15, False),
    ('bateria', 'Bateria', 'Texto', 49, False),
    ('valor_bem', 'Valor do Bem', 'Texto', 17, False),
    ('revendedora', 'Revendedora', 'Texto', 100, False),
    ('codigo_revendedora', 'Código da Revendedora', 'Texto', 10, False),
    ('ultima_concessionaria_exec', 'Última Concessionária Exec.', 'Texto', 100, False),
    ('data_venda', 'Data da Venda', 'Data', 10, False),
    ('data_inicial_garantia', 'Data Inicial Garantia', 'Data', 10, False),
    ('data_final_garantia', 'Data Final Garantia', 'Data', 10, False),
    ('rg', 'RG', 'Texto', 17, False),
    ('uf_rg', 'UF RG', 'Numérico', 2, False),
    ('numero_produtor_rural', 'Número do Produtor Rural', 'Texto', 20, False),
    ('id_estrangeiro', 'ID Estrangeiro', 'Texto', 20, False),
    ('data_hora_ultima_alteracao', 'Data/Hora última alteração', 'Timestamp', 19, False),
    ('inscricao_estadual', 'Inscrição Estadual', 'Texto', 17, False),
    ('uf_inscricao_estadual', 'UF Inscrição estadual', 'Numérico', 2, False),
]

CAMPOS_IGNORAR_HISTORY_IA = [
    'preco_venda', 'preco_custo_aquisicao', 'preco_venda_sugerido', 'preco_garantia',
    'preco_custo_fabrica', 'origem', 'qtd_embalagem', 'custo_medio', 'custo_medio_contabil','custo_ultima_compra',
    'base_media_icms_st', 'valor_medio_icms_st', 'saldo', 'custo_contabil_ultima_compra'
]

LAYOUTS = {
    "mercadorias": {
        "nome": "Cadastro de Mercadorias",
        "layout": LAYOUT_MERCADORIA,
        "js": "mercadorias.js",
        "keywords": {
            'codigo': ['codigo', 'código', 'sku', 'product_code', 'ean', 'cod', 'código_mercadoria', 'código mercadoria'],
            'nome': ['nome', 'descricao', 'descrição', 'nome_produto', 'item'],
            'unidade': ['unidade', 'unid', 'unit', 'und', 'u.m', 'um'],
            'marca': ['marca', 'brand', 'fabricante'],
            'tipo': ['tipo', 'categoria', 'grupo', 'tipomercadoria', 'tipo_mercadoria', 'segmento'],
            'ncm': ['ncm', 'codncm'],
            'tributacao': ['tributacao', 'cst', 'trib', 'tributação', 'trib_estadual', 'tributação_estadual', 'trib_est', 'situacao_tributaria', 'sit_trib'],
            'preco_venda': ['preco_venda', 'valorvenda', 'venda', 'pvenda', 'preço', 'preco'],
            'preco_custo_aquisicao': ['preco_custo', 'custo', 'aquisicao', 'pcusto', 'preco_compra', 'custoaquisicao'],
            'original': ['original', 'genuino', 'oem'],
            'aplicacao': ['aplicacao', 'aplicação', 'uso'],
            'origem': ['origem', 'procedencia'],
            'anp': ['anp'],
            'coeficiente': ['coeficiente', 'fator'],
            'qtd_embalagem': ['qtd_embalagem', 'quantidade_embalagem', 'qtdembalagem', 'caixa'],
            'curva_abc': ['curva_abc', 'abc'],
            'curva_xyz': ['curva_xyz', 'xyz'],
            'cod_original': ['cod_original', 'codigooriginal'],
            'cest': ['cest', 'codcest'],
            'preco_venda_sugerido': ['preco_venda_sugerido', 'sugerido', 'pv_sugerido'],
            'preco_garantia': ['preco_garantia', 'garantia'],
            'preco_custo_fabrica': ['preco_custo_fabrica', 'custo_fabrica', 'pcf'],
        }
    },
    "mercadorias_saldos": {
        "nome": "Saldo de Mercadorias",
        "layout": LAYOUT_MERCADORIA_SALDOS,
        "js": "mercadorias_saldos.js",
        "keywords": {
            'codigo': ['codigo', 'código', 'sku', 'ean', 'product_code', 'código mercadoria', 'código_mercadoria', 'cod_merc'],
            'tipo_localizacao': ['tipo_localizacao', 'tipolocalizacao', 'tipo localizacao', 'tipo loc', 'tipo'],
            'localizacao': ['localizacao', 'localização', 'local', 'prateleira', 'deposito'],
            'custo_medio': ['custo_medio', 'custo medio', 'medcost', 'costavg'],
            'custo_medio_contabil': ['custo_medio_contabil', 'custo medio contabil', 'contabil avg cost'],
            'custo_ultima_compra': ['custo_ultima_compra', 'custo ultima compra', 'last_cost'],
            'base_media_icms_st': ['base_media_icms_st', 'base media icms st', 'icms st base'],
            'valor_medio_icms_st': ['valor_medio_icms_st', 'valor medio icms st', 'icms st valor'],
            'saldo': ['saldo', 'saldo_atual', 'quantidade', 'qty', 'estoque'],
            'custo_contabil_ultima_compra': ['custo_contabil_ultima_compra', 'custo contabil ultima compra', 'last_cost_contabil'],
        }
    },
    "pessoas": {
        "nome": "Cadastro de Pessoas",
        "layout": LAYOUT_PESSOAS,
        "js": "pessoas.js",
        "keywords": {
            "cpf_cnpj": ["cpf", "cnpj", "documento", "cpf/cnpj", "cpf_cnpj"],
            "nome_razao": ["nome", "razão", "razao", "nome_razao", "nome/razao", "razao_social", "nome social"],
            "apelido_fantasia": ["apelido", "fantasia", "nome fantasia", "nome_fantasia"],
            "rg": ["rg", "registro geral"],
            "uf_rg": ["uf rg", "uf_rg"],
            "inscricao_municipal": ["inscricao_municipal", "inscrição municipal", "im"],
            "tipo_pessoa": ["tipo pessoa", "tipo_pessoa", "tpessoa", "tipo"],
            "tipo_contribuinte": ["tipo contribuinte", "tipo_contribuinte", "tcontribuinte","contribuinte","tipocontribuinte"],
            "sexo": ["sexo", "genero"],
            "estado_civil": ["estado civil", "estado_civil"],
            "nacionalidade": ["nacionalidade", "pais", "nacao"],
            "data_nascimento": ["data nascimento", "nascimento", "dt_nasc", "data_nascimento"],
            "data_emancipacao": ["data emancipacao", "emancipacao", "data_emancipacao"],
            "tipo_endereco": ["tipo endereco", "tipo_endereco"],
            "cep": ["cep", "codigo postal"],
            "logradouro": ["logradouro", "rua", "endereço"],
            "numero_endereco": ["numero endereco", "numero", "número", "num_endereco"],
            "bairro": ["bairro"],
            "municipio": ["municipio", "cidade", "municipality", "município"],
            "uf": ["uf", "estado"],
            "complemento_endereco": ["complemento", "complemento endereco"],
            "tipo_telefone": ["tipo telefone", "tipo_telefone"],
            "ddi_telefone": ["ddi", "DDI  TELEFONE"],
            "ddd_telefone": ["ddd", "DDD TELEFONE"],
            "telefone": ["TELEFONE", "celular", "fone celular"],
            "ramal": ["ramal", "ramal"],
            "contato": ["contato", "contato"],
            "email": ["email", "email", "e-mail"],
            "numero_produtor_rural": ["nº produtor rural", "numero produtor rural", "rural", "prod_rural"],
            "data_limite_credito": ["data limite credito", "data_limite_credito"],
            "valor_limite_credito": ["valor limite credito", "valor_limite_credito"],
            "finalidade_contato": ["finalidade contato", "finalidade", "motivo contato"],
            "ie": ["ie", "inscricao estadual"],
            "uf_ie": ["uf ie"],
            "produtor_rural": ["produtor rural", "produtor_rural"],
        }
    },
    "veiculos_cliente": {
        "nome": "Cadastro de Veículos do Cliente",
        "layout": LAYOUT_VEICULO_CLIENTE,
        "js": "veiculos_cliente.js",
        "keywords": {
            'cpf_cnpj': ['cpf', 'cnpj', 'cpf_cnpj', 'cpf/cnpj', 'documento'],
            'placa': ['placa', 'placa_veiculo'],
            'modelo': ['modelo', 'modelo_veiculo'],
            'cor': ['cor'],
            'ano_fabricacao': ['ano_fabricacao', 'ano de fabricacao', 'ano fabr', 'ano_fabricação'],
            'ano_modelo': ['ano_modelo', 'ano do modelo'],
            'chassi': ['chassi'],
            'motor': ['motor'],
            'renavam': ['renavam'],
            'crlv': ['crlv'],
            'bateria': ['bateria'],
            'valor_bem': ['valor_bem', 'valor do bem', 'valor_bem_veiculo'],
            'revendedora': ['revendedora', 'concessionaria', 'loja'],
            'codigo_revendedora': ['codigo_revendedora', 'cod_revendedora'],
            'ultima_concessionaria_exec': ['ultima_concessionaria_exec', 'última concessionária exec', 'ultima concessionaria', 'ult_concessionaria'],
            'data_venda': ['data_venda', 'data da venda', 'dt_venda'],
            'data_inicial_garantia': ['data_inicial_garantia', 'data inicial garantia', 'dt_ini_garantia'],
            'data_final_garantia': ['data_final_garantia', 'data final garantia', 'dt_fim_garantia'],
            'rg': ['rg', 'registro geral'],
            'uf_rg': ['uf_rg', 'uf rg'],
            'numero_produtor_rural': ['numero_produtor_rural', 'número produtor rural'],
            'id_estrangeiro': ['id_estrangeiro', 'id estrangeiro'],
            'data_hora_ultima_alteracao': ['data_hora_ultima_alteracao', 'data hora ultima alteracao'],
            'inscricao_estadual': ['inscricao_estadual', 'inscrição estadual'],
            'uf_inscricao_estadual': ['uf_inscricao_estadual', 'uf inscricao estadual'],
        }
    },
}

with app.app_context():
    init_db()

def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return str(obj)
    return obj

def load_mapping_history(tipo_layout):
    """Carrega as amostras da tabela de layout específica e as agrupa por campo."""
    conn = get_db()
    if conn is None:
        return {}

    table_name = tipo_layout
    history_data = {}
    query = f"""
        SELECT field_name, array_agg(sample_value)
        FROM {table_name}
        GROUP BY field_name;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
            for row in results:
                field_name, samples = row
                history_data[field_name] = {"amostras_validas": samples or []}
        return history_data
    except psycopg2.errors.UndefinedTable:
        # A tabela pode não existir ainda, o que é normal na primeira execução.
        conn.rollback()
        return {}
    except Exception as e:
        print(f"Erro ao carregar histórico de '{table_name}': {e}")
        return {}

def save_mapping_history(tipo_layout, novas_amostras_data):
    """Insere novas amostras na tabela de layout apropriada, uma por uma."""
    conn = get_db()
    if conn is None or not novas_amostras_data:
        return

    table_name = tipo_layout
    query = f"""
        INSERT INTO {table_name} (field_name, sample_value)
        VALUES (%s, %s)
        ON CONFLICT (field_name, sample_value) DO NOTHING;
    """

    try:
        with conn.cursor() as cur:
            for field, data in novas_amostras_data.items():
                for sample in data.get("amostras_validas", []):
                    cur.execute(query, (field, sample))
        conn.commit()
    except Exception as e:
        print(f"Erro ao salvar amostra em '{table_name}': {e}")
        conn.rollback()

# ... (rest of the file remains the same, so it's omitted for brevity)
# I will just copy the rest of the original file content here
def normalizar_nome(nome):
    return re.sub(r'[^a-z0-9]', '', str(nome).lower())

def similaridade(a, b):
    return fuzz.ratio(a, b) / 100.0

def limpar_uploads():
    pasta = app.config['UPLOAD_FOLDER']
    for f in os.listdir(pasta):
        caminho_f = os.path.join(pasta, f)
        try:
            if os.path.isfile(caminho_f):
                os.remove(caminho_f)
        except Exception:
            pass

def detectar_encoding_e_linhas_validas(file_bytes, extensao='.csv', filename='arquivo'):
    if extensao == '.xlsx':
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
            df = normalizar_colunas_vazias(df)
            return df, None, None, []
        except Exception:
            return None, None, None, []

    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1', 'windows-1252']
    delimiters = [',', ';', '|', '\t']
    melhor_df, melhor_sep, melhor_enc, maior_colunas = None, None, None, 0
    linhas_ignoradas_indices = []

    for enc in encodings:
        try:
            texto = file_bytes.decode(enc)
        except Exception:
            continue

        try:
            sniffer = csv.Sniffer()
            sample = texto[:2048]
            dialect = sniffer.sniff(sample, delimiters=delimiters)
            sep_heur = dialect.delimiter
        except Exception:
            first_lines = "\n".join(texto.splitlines()[:10])
            sep_counts = {sep: first_lines.count(sep) for sep in delimiters}
            sep_heur = max(sep_counts, key=sep_counts.get)
        seps_to_try = [sep_heur] + [s for s in delimiters if s != sep_heur]

        for sep in seps_to_try:
            try:
                multiline_lines = []
                in_quotes = False
                line_num = 0
                with io.StringIO(texto) as f:
                    for raw_line in f:
                        line_num += 1
                        quote_count = len(re.findall(r'(?<!")"(?!")', raw_line))
                        if quote_count % 2 == 1:
                            in_quotes = not in_quotes
                            multiline_lines.append(line_num)
                        elif in_quotes:
                            multiline_lines.append(line_num)
                multiline_lines = sorted(set(multiline_lines))

                reader = csv.reader(io.StringIO(texto), delimiter=sep, quotechar='"')
                all_lines = texto.splitlines(keepends=True)
                rows = []
                linhas_ignoradas = set()
                header = None
                n_cols = None
                idx_linha_arq = 0
                for row in csv.reader(io.StringIO(texto), delimiter=sep, quotechar='"'):
                    idx_linha_arq += 1
                    if header is None and any(str(f).strip() != '' for f in row):
                        header = row
                        n_cols = len(header)
                        rows.append(row)
                    elif not row or all([str(f).strip() == '' for f in row]):
                        continue
                    elif len(row) != n_cols:
                        linhas_ignoradas.add(idx_linha_arq)
                    else:
                        rows.append(row)

                linhas_validas = []
                current_line = 0
                for row in csv.reader(io.StringIO(texto), delimiter=sep, quotechar='"'):
                    current_line += 1
                    if current_line in linhas_ignoradas:
                        continue
                    linhas_validas.append(row)
                output = io.StringIO()
                writer = csv.writer(output, delimiter=sep, quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in linhas_validas:
                    writer.writerow(row)
                texto_validado = output.getvalue()

                df = pd.read_csv(
                    io.StringIO(texto_validado),
                    sep=sep,
                    quotechar='"',
                    engine='python',
                    keep_default_na=False,
                    dtype=str,
                    on_bad_lines='skip'
                )
                df = normalizar_colunas_vazias(df)

                if df.shape[1] > maior_colunas and df.shape[1] > 1 and len(df) > 0:
                    melhor_df = df
                    melhor_sep = sep
                    melhor_enc = enc
                    maior_colunas = df.shape[1]
                    alertas = []
                    if multiline_lines:
                        exemplo = ', '.join(str(x) for x in multiline_lines[:8])
                        alertas.append(
                            f'Atenção: O arquivo "{filename}" contém {len(multiline_lines)} linha(s) com quebra de linha. '
                            f'Essas linhas foram ignoradas na análise para evitar inconsistências. Linhas: {exemplo}.'
                        )
                    if linhas_ignoradas:
                        exemplo = ', '.join(str(x) for x in list(linhas_ignoradas)[:8])
                        alertas.append(
                            f'Atenção: O arquivo "{filename}" contém {len(linhas_ignoradas)} linha(s) com quebra de linha. '
                            f'Essas linhas foram ignoradas na análise para evitar inconsistências. Linhas: {exemplo}.'
                        )
                    linhas_ignoradas_indices = alertas
            except Exception:
                continue
        if melhor_df is not None:
            break

    return melhor_df, melhor_sep, melhor_enc, linhas_ignoradas_indices

def normalizar_colunas_vazias(df):
    new_cols = []
    empty_count = 1
    for i, col in enumerate(df.columns):
        nome = str(col)
        if nome.strip() == '' or nome.lower().startswith('unnamed:'):
            new_cols.append(f'(sem nome {empty_count})')
            empty_count += 1
        else:
            new_cols.append(nome)
    df.columns = new_cols
    return df


# --------- MERCADORIAS CADASTRO --------- #

def validar_campo_mercadorias(campo, valor):
    if isinstance(valor, str):
        valor_limpo = valor.strip()
    else:
        valor_limpo = valor
    layout_info = next((item for item in LAYOUT_MERCADORIA if item[0] == campo), None)
    if layout_info and layout_info[4] and (pd.isna(valor_limpo) or str(valor_limpo).strip() == ''):
        return False
    if not layout_info[4] and (pd.isna(valor_limpo) or str(valor_limpo).strip() == ''):
        return True
    max_len = layout_info[3] if layout_info and isinstance(layout_info[3], int) else float('inf')
    if campo == 'codigo':
        try:
            s = str(valor_limpo)
            if not (4 <= len(s) <= max_len and re.match(r'^[A-Z0-9\s\-\/\.]+$', s, re.I)):
                return False
            common_words = ['de', 'da', 'do', 'com', 'para', 'em', 'um', 'uma']
            if any(word in s.lower().split() for word in common_words):
                return False
            return True
        except Exception:
            return False
    if campo == 'nome':
        return isinstance(valor_limpo, str) and 5 <= len(str(valor_limpo)) <= 150
    if campo == 'unidade':
        return isinstance(valor_limpo, str) and 1 <= len(str(valor_limpo)) <= max_len
    if campo == 'marca':
        return isinstance(valor_limpo, str) and 1 <= len(str(valor_limpo)) <= max_len
    if campo == 'tipo':
        return isinstance(valor_limpo, str) and 1 <= len(str(valor_limpo)) <= max_len
    if campo == 'ncm':
        if isinstance(valor_limpo, (int, float)):
            valor_limpo = str(int(valor_limpo))
        return (isinstance(valor_limpo, str)
            and (len(str(valor_limpo)) == 8 or (len(str(valor_limpo)) == 10 and '.' in str(valor_limpo)))
            and re.match(r'^[0-9\.]+$', str(valor_limpo)))
    if campo == 'tributacao':
        return isinstance(valor_limpo, str) and 1 <= len(str(valor_limpo)) <= max_len
    if campo in ['preco_venda', 'preco_custo_aquisicao', 'preco_venda_sugerido', 'preco_garantia', 'preco_custo_fabrica']:
        try:
            num = float(str(valor_limpo).replace(',', '.'))
            return num >= 0
        except ValueError:
            return False
    if campo == 'original':
        return str(valor_limpo).strip().lower() in ['1', '0', 'true', 'false', 'sim', 'não', 'nao', 'yes', 'no']
    if campo in ['curva_abc', 'curva_xyz']:
        return str(valor_limpo).strip().upper() in ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']
    if campo == 'aplicacao':
        return isinstance(valor_limpo, str) and len(str(valor_limpo)) <= max_len
    if campo in ['origem', 'anp', 'qtd_embalagem']:
        if campo == 'anp':
            v = str(valor_limpo).strip()
            if v == "":
                return True
            if v.isdigit() and len(v) == 9:
                return True
            try:
                v_int = int(float(v))
                return len(str(v_int)) == 9
            except Exception:
                return False
        else:
            try:
                return str(valor_limpo).strip() == "" or float(str(valor_limpo).replace(',', '.')) >= 0
            except ValueError:
                return False
    if campo == 'coeficiente':
        return isinstance(valor_limpo, str) and len(str(valor_limpo)) <= max_len
    if campo == 'cod_original':
        return isinstance(valor_limpo, str) and len(str(valor_limpo)) <= max_len
    if campo == 'cest':
        if isinstance(valor_limpo, (int, float)):
            valor_limpo = str(int(valor_limpo))
        return (isinstance(valor_limpo, str)
            and (len(str(valor_limpo)) == 7 or (len(str(valor_limpo)) == 9 and '.' in str(valor_limpo)))
            and re.match(r'^[0-9\.]+$', str(valor_limpo)))
    return True

def auto_map_header_mercadorias(df, layout, keywords):
    auto_map = {}
    for campo, *_ in layout:
        melhor_col, melhor_score = None, 0
        for col in df.columns:
            col_norm = normalizar_nome(col)
            for key in keywords.get(campo, [campo]):
                key_norm = normalizar_nome(key)
                score = similaridade(col_norm, key_norm)
                if score > melhor_score and score >= 0.82:
                    melhor_col, melhor_score = col, score
        if melhor_col:
            auto_map[campo] = melhor_col
    return auto_map

def auto_map_by_data_mercadorias(df, layout, mapping_history):
    auto_map = {}
    for campo, *_ in layout:
        if campo in mapping_history and mapping_history[campo].get("amostras_validas"):
            campo_amostras = set(mapping_history[campo]["amostras_validas"])
            melhor_col, melhor_score = None, 0
            for col in df.columns:
                serie = df[col].dropna().astype(str)
                serie_validas = set([v for v in serie if validar_campo_mercadorias(campo, v)])
                if not serie_validas:
                    continue
                intersecao = campo_amostras.intersection(serie_validas)
                score = len(intersecao) / max(len(serie_validas), 1)
                if score > melhor_score and score >= 0.5:
                    melhor_col, melhor_score = col, score
            if melhor_col:
                auto_map[campo] = melhor_col
    return auto_map

def is_vazio(v):
    if v is None:
        return True
    if isinstance(v, float):
        try:
            if math.isnan(v):
                return True
        except Exception:
            pass

    if isinstance(v, str):
        if v.strip() == "" or v.strip().lower() == "nan":
            return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    return False

def aprender_metadados_coluna_mercadorias(serie, campo_layout, old_samples=None):
    if campo_layout in CAMPOS_IGNORAR_HISTORY_IA:
        return old_samples or []
    valid_samples = set(s for s in (old_samples or []) if s != "")
    for val in pd.Series(serie.dropna().astype(str).unique()):
        if validar_campo_mercadorias(campo_layout, val) and val != "":
            valid_samples.add(val)
    return list(valid_samples)

def analisar_dados_mercadorias(df, layout, mapeamento):
    inconsistencias = {}
    stats = []
    total_linhas = len(df)
    linha_valida = [True] * total_linhas

    def is_vazio(v):
        if v is None:
            return True
        if isinstance(v, float):
            try:
                if math.isnan(v):
                    return True
            except Exception:
                pass
        if isinstance(v, str):
            if v.strip() == "" or v.strip().lower() == "nan":
                return True
        try:
            if pd.isna(v):
                return True
        except Exception:
            pass
        return False

    codigos_em_branco = []
    exemplos_codigo_em_branco = []
    codigos_duplicados = set()
    codigos_com_espaco = set()
    codigos_ultrapassa_tamanho = set()
    codigos_vistos = set()
    exemplos_codigo_duplicados = []
    exemplos_codigo_espaco = []
    exemplos_codigo_ultrapassa = []

    codigo_col = mapeamento.get('codigo')
    if codigo_col and codigo_col in df.columns:
        maxlen_codigo = [x[3] for x in layout if x[0] == 'codigo'][0] if [x[3] for x in layout if x[0] == 'codigo'] else 20

        for idx, v in enumerate(df[codigo_col]):
            if is_vazio(v):
                codigos_em_branco.append("")
                if len(exemplos_codigo_em_branco) < 8:
                    exemplos_codigo_em_branco.append("")
                linha_valida[idx] = False
            else:
                valor = str(v).strip()
                if valor in codigos_vistos:
                    codigos_duplicados.add(valor)
                    if len(exemplos_codigo_duplicados) < 8:
                        exemplos_codigo_duplicados.append(valor)
                    linha_valida[idx] = False
                else:
                    codigos_vistos.add(valor)
                if ' ' in valor:
                    codigos_com_espaco.add(valor)
                    if len(exemplos_codigo_espaco) < 8:
                        exemplos_codigo_espaco.append(valor)
                if len(valor) > maxlen_codigo:
                    codigos_ultrapassa_tamanho.add(valor)
                    if len(exemplos_codigo_ultrapassa) < 8:
                        exemplos_codigo_ultrapassa.append(valor)
                    linha_valida[idx] = False

    if codigos_em_branco:
        inconsistencias['codigo_em_branco'] = {
            "label": "Código",
            "tipo": "em_branco",
            "mensagem": f"Em branco: {len(codigos_em_branco)} registro(s)",
            "amostra": []
        }
    if codigos_duplicados:
        inconsistencias['codigo_duplicado'] = {
            "label": "Código",
            "tipo": "duplicado",
            "mensagem": f"Duplicados: {len(codigos_duplicados)} registro(s)",
            "amostra": exemplos_codigo_duplicados
        }
    if codigos_ultrapassa_tamanho:
        inconsistencias['codigo_ultrapassa'] = {
            "label": "Código",
            "tipo": "ultrapassa_tamanho",
            "mensagem": f"Excede o limite de caracteres: {len(codigos_ultrapassa_tamanho)} registro(s)",
            "amostra": exemplos_codigo_ultrapassa
        }

    texto_ultrapassa = {}
    texto_nao_texto = {}
    negativos = {}

    for campo, label, tipo, tamanho, obrigatorio in layout:
        col = mapeamento.get(campo)
        if obrigatorio and (not col or col not in df.columns):
            inconsistencias[campo] = {
                "label": label,
                "tipo": "invalido",
                "mensagem": "Campo obrigatório não mapeado ou não encontrado.",
                "amostra": []
            }
            stats.append({'campo': label, 'validos': 0, 'invalidos': total_linhas})
            linha_valida = [False] * total_linhas
            continue
        if not col or col not in df.columns:
            stats.append({'campo': label, 'validos': 0, 'invalidos': 0})
            continue

        serie = df[col].astype(str)
        validos, invalidos = 0, 0
        em_branco_qtd = 0
        exemplos_em_branco = []
        exemplos_outros_invalidos = []
        is_numeric = campo in ['preco_venda', 'preco_custo_aquisicao', 'preco_venda_sugerido',
                              'preco_garantia', 'preco_custo_fabrica']

        for idx, v in enumerate(serie):
            vazio = is_vazio(v)
            if tipo.lower() == 'texto':
                maxlen = [x[3] for x in layout if x[0] == campo][0]
                if not isinstance(v, str):
                    texto_nao_texto.setdefault(campo, set()).add(v)
                if maxlen and isinstance(v, str) and v.strip() != "" and len(str(v)) > int(maxlen):
                    texto_ultrapassa.setdefault(campo, set()).add(v)

            valido = validar_campo_mercadorias(campo, v)
            if is_numeric:
                try:
                    num = float(str(v).replace(',', '.'))
                    if num < 0:
                        if campo not in negativos:
                            negativos[campo] = []
                        negativos[campo].append(v)
                except Exception:
                    pass

            if not valido:
                invalidos += 1
                if vazio:
                    em_branco_qtd += 1
                    if len(exemplos_em_branco) < 8:
                        exemplos_em_branco.append(v)
                else:
                    if len(exemplos_outros_invalidos) < 8 and not is_vazio(v):
                        exemplos_outros_invalidos.append(v)
                if obrigatorio:
                    linha_valida[idx] = False
            else:
                validos += 1

        stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})

        if is_numeric and campo in negativos:
            exemplos_negativos_ordenados = sorted(
                set([v for v in negativos[campo] if not is_vazio(v)]),
                key=lambda x: float(str(x).replace(',', '.'))
            )
            inconsistencias[campo] = {
                "label": label,
                "tipo": "negativo",
                "mensagem": f"Valor negativo: {len(negativos[campo])} registro(s)",
                "amostra": exemplos_negativos_ordenados
            }
        elif invalidos - em_branco_qtd > 0:
            exemplos_distintos_ordenados = sorted(
                set([v for v in exemplos_outros_invalidos if not is_vazio(v)]),
                key=lambda x: (str(x).lower(), str(x))
            )
            inconsistencias[campo] = {
                "label": label,
                "tipo": "invalido",
                "mensagem": f"Valor inválido: {invalidos - em_branco_qtd} registro(s)",
                "amostra": exemplos_distintos_ordenados
            }

        if em_branco_qtd > 0:
            inconsistencias[f'{campo}_em_branco'] = {
                "label": label,
                "tipo": "em_branco",
                "mensagem": f"Em branco: {em_branco_qtd} registro(s)",
                "amostra": []
            }

    for campo, valores in texto_ultrapassa.items():
        label = [x[1] for x in layout if x[0] == campo][0]
        inconsistencias[f'{campo}_ultrapassa'] = {
            "label": label,
            "tipo": "ultrapassa_tamanho",
            "mensagem": f"Excede o limite de caracteres: {len(valores)} registro(s)",
            "amostra": sorted(
                [v for v in valores if not is_vazio(v)],
                key=lambda x: (str(x).lower(), str(x))
            )
        }

    for campo, valores in texto_nao_texto.items():
        label = [x[1] for x in layout if x[0] == campo][0]
        inconsistencias[f'{campo}_nao_texto'] = {
            "label": label,
            "tipo": "nao_texto",
            "mensagem": f"Valor não é texto: {len(valores)} registro(s)",
            "amostra": sorted(
                [v for v in valores if not is_vazio(v)],
                key=lambda x: (str(x).lower(), str(x))
            )
        }

    inconsistencias_ordenadas = dict(sorted(inconsistencias.items(), key=lambda x: x[1]['label'].lower()))
    total_validos_geral = sum(linha_valida)
    total_invalidos_geral = total_linhas - total_validos_geral

    return inconsistencias_ordenadas, stats, total_linhas, total_validos_geral, total_invalidos_geral

# --------------- MERCADORIAS SALDOS --------------- #

def validar_campo_mercadorias_saldos(campo, valor):
    if isinstance(valor, str):
        valor_limpo = valor.strip()
    else:
        valor_limpo = valor
    layout_info = next((item for item in LAYOUT_MERCADORIA_SALDOS if item[0] == campo), None)
    if layout_info and layout_info[4] and (pd.isna(valor_limpo) or str(valor_limpo).strip() == ''):
        return False
    if not layout_info[4] and (pd.isna(valor_limpo) or str(valor_limpo).strip() == ''):
        return True
    max_len = layout_info[3] if layout_info and isinstance(layout_info[3], int) else float('inf')
    if campo == 'codigo':
        return isinstance(valor_limpo, str) and 1 <= len(str(valor_limpo)) <= max_len
    if campo in ['tipo_localizacao', 'localizacao']:
        return isinstance(valor_limpo, str) and len(str(valor_limpo)) <= max_len
    if campo in ['custo_medio', 'custo_medio_contabil', 'custo_ultima_compra', 'base_media_icms_st', 'valor_medio_icms_st', 'saldo', 'custo_contabil_ultima_compra']:
        try:
            float(str(valor_limpo).replace(',', '.'))
            return True
        except:
            return False
    return True

def auto_map_header_mercadorias_saldos(df, layout, keywords):
    auto_map = {}
    for campo, *_ in layout:
        melhor_col, melhor_score = None, 0
        for col in df.columns:
            col_norm = normalizar_nome(col)
            for key in keywords.get(campo, [campo]):
                key_norm = normalizar_nome(key)
                score = similaridade(col_norm, key_norm)
                if score > melhor_score and score >= 0.82:
                    melhor_col, melhor_score = col, score
        if melhor_col:
            auto_map[campo] = melhor_col
    return auto_map

def auto_map_by_data_mercadorias_saldos(df, layout, mapping_history):
    auto_map = {}
    for campo, *_ in layout:
        if campo in mapping_history and mapping_history[campo].get("amostras_validas"):
            campo_amostras = set(mapping_history[campo]["amostras_validas"])
            melhor_col, melhor_score = None, 0
            for col in df.columns:
                serie = df[col].dropna().astype(str)
                serie_validas = set([v for v in serie if validar_campo_mercadorias_saldos(campo, v)])
                if not serie_validas:
                    continue
                intersecao = campo_amostras.intersection(serie_validas)
                score = len(intersecao) / max(len(serie_validas), 1)
                if score > melhor_score and score >= 0.5:
                    melhor_col, melhor_score = col, score
            if melhor_col:
                auto_map[campo] = melhor_col
    return auto_map

def aprender_metadados_coluna_mercadorias_saldos(serie, campo_layout, old_samples=None):
    if campo_layout in CAMPOS_IGNORAR_HISTORY_IA:
        return old_samples or []
    valid_samples = set(s for s in (old_samples or []) if s != "")
    for val in pd.Series(serie.dropna().astype(str).unique()):
        if validar_campo_mercadorias_saldos(campo_layout, val) and val != "":
            valid_samples.add(val)
    return list(valid_samples)

def analisar_dados_mercadorias_saldos(df, layout, mapeamento):
    def is_vazio(v):
        if v is None:
            return True
        if isinstance(v, float):
            try:
                if math.isnan(v):
                    return True
            except Exception:
                pass
        if isinstance(v, str):
            if v.strip() == "" or v.strip().lower() == "nan":
                return True
        try:
            if pd.isna(v):
                return True
        except Exception:
            pass
        return False

    def is_numero_valido(valor):
        valor = str(valor).strip().replace(" ", "")
        regex = r'^[+-]?(\d+([.,]\d*)?|[.,]\d+)$'
        return bool(re.match(regex, valor))

    inconsistencias = {}
    stats = []
    total_linhas = len(df)
    linha_valida = [True] * total_linhas

    codigos_em_branco = []
    exemplos_codigo_em_branco = []
    codigos_duplicados = set()
    codigos_ultrapassa_tamanho = set()
    codigos_com_espaco = set()
    exemplos_codigo_duplicados = []
    exemplos_codigo_ultrapassa = []
    exemplos_codigo_espaco = []

    codigos_vistos = set()
    codigo_col = mapeamento.get('codigo')
    if codigo_col and codigo_col in df.columns:
        maxlen_codigo = [x[3] for x in layout if x[0] == 'codigo'][0]
        for idx, v in enumerate(df[codigo_col]):
            if is_vazio(v):
                codigos_em_branco.append("")
                if len(exemplos_codigo_em_branco) < 8:
                    exemplos_codigo_em_branco.append("")
                linha_valida[idx] = False
            else:
                valor = str(v).strip()
                if valor in codigos_vistos:
                    codigos_duplicados.add(valor)
                    if len(exemplos_codigo_duplicados) < 8:
                        exemplos_codigo_duplicados.append(valor)
                    linha_valida[idx] = False
                else:
                    codigos_vistos.add(valor)
                if ' ' in valor:
                    codigos_com_espaco.add(valor)
                    if len(exemplos_codigo_espaco) < 8:
                        exemplos_codigo_espaco.append(valor)
                if len(valor) > maxlen_codigo:
                    codigos_ultrapassa_tamanho.add(valor)
                    if len(exemplos_codigo_ultrapassa) < 8:
                        exemplos_codigo_ultrapassa.append(valor)
                    linha_valida[idx] = False

    # Mensagens padronizadas
    if codigos_em_branco:
        inconsistencias['codigo_em_branco'] = {
            "label": "Código",
            "tipo": "em_branco",
            "mensagem": f"Em branco: {len(codigos_em_branco)} registro(s)",
            "amostra": []
        }
    if codigos_duplicados:
        inconsistencias['codigo_duplicado'] = {
            "label": "Código",
            "tipo": "duplicado",
            "mensagem": f"Duplicados: {len(codigos_duplicados)} registro(s)",
            "amostra": exemplos_codigo_duplicados
        }
    if codigos_ultrapassa_tamanho:
        inconsistencias['codigo_ultrapassa'] = {
            "label": "Código",
            "tipo": "ultrapassa_tamanho",
            "mensagem": f"Excede o limite de caracteres: {len(codigos_ultrapassa_tamanho)} registro(s)",
            "amostra": exemplos_codigo_ultrapassa
        }
    if codigos_com_espaco:
        inconsistencias['codigo_com_espaco'] = {
            "label": "Código",
            "tipo": "com_espaco",
            "mensagem": f"Possui espaço(s) indevido(s): {len(codigos_com_espaco)} registro(s)",
            "amostra": exemplos_codigo_espaco
        }

    texto_ultrapassa = {}
    negativos = {}
    nao_numericos = {}
    zerados = {}

    for campo, label, tipo, tamanho, obrigatorio in layout:
        col = mapeamento.get(campo)
        if obrigatorio and (not col or col not in df.columns):
            inconsistencias[campo] = {
                "label": label,
                "tipo": "invalido",
                "mensagem": "Campo obrigatório não mapeado ou não encontrado.",
                "amostra": []
            }
            stats.append({'campo': label, 'validos': 0, 'invalidos': total_linhas})
            linha_valida = [False] * total_linhas
            continue
        if not col or col not in df.columns:
            stats.append({'campo': label, 'validos': 0, 'invalidos': 0})
            continue

        serie = df[col].astype(str)
        validos, invalidos = 0, 0
        em_branco_qtd = 0
        exemplos_em_branco = []
        exemplos_outros_invalidos = []
        exemplos_zerados = []
        exemplos_nao_numericos = []
        is_numeric = tipo.lower() == 'numérico' or campo in [
            'custo_medio', 'custo_medio_contabil', 'custo_ultima_compra',
            'base_media_icms_st', 'valor_medio_icms_st', 'saldo', 'custo_contabil_ultima_compra'
        ]

        for idx, v in enumerate(serie):
            vazio = is_vazio(v)
            if tipo.lower() == 'texto':
                maxlen = [x[3] for x in layout if x[0] == campo][0]
                if maxlen and isinstance(v, str) and v.strip() != "" and len(str(v)) > int(maxlen):
                    texto_ultrapassa.setdefault(campo, set()).add(v)
            if is_numeric:
                if vazio:
                    em_branco_qtd += 1
                    if len(exemplos_em_branco) < 8:
                        exemplos_em_branco.append(v)
                    continue
                if not is_numero_valido(v):
                    invalidos += 1
                    nao_numericos.setdefault(campo, []).append(v)
                    exemplos_nao_numericos.append(v)
                    if obrigatorio:
                        linha_valida[idx] = False
                    continue
                try:
                    num = float(str(v).replace(',', '.'))
                    if num == 0:
                        zerados.setdefault(campo, []).append(v)
                        if len(exemplos_zerados) < 8:
                            exemplos_zerados.append(v)
                        validos += 1
                        continue
                    if campo not in ['custo_contabil_ultima_compra']:
                        if num < 0:
                            negativos.setdefault(campo, []).append(v)
                    validos += 1
                except Exception:
                    invalidos += 1
                    nao_numericos.setdefault(campo, []).append(v)
                    exemplos_nao_numericos.append(v)
                    if obrigatorio:
                        linha_valida[idx] = False
                continue

            valido = True
            if tipo.lower() == 'texto':
                maxlen = [x[3] for x in layout if x[0] == campo][0]
                if maxlen and len(str(v)) > int(maxlen):
                    valido = False

            if not valido:
                invalidos += 1
                if vazio:
                    em_branco_qtd += 1
                    if len(exemplos_em_branco) < 8:
                        exemplos_em_branco.append(v)
                else:
                    if len(exemplos_outros_invalidos) < 8 and not is_vazio(v):
                        exemplos_outros_invalidos.append(v)
                if obrigatorio:
                    linha_valida[idx] = False
            else:
                validos += 1

        stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})

        # Mensagens padronizadas para inconsistências
        if is_numeric and campo in nao_numericos and nao_numericos[campo]:
            inconsistencias[f'{campo}_nao_numerico'] = {
                "label": label,
                "tipo": "invalido",
                "mensagem": f"Valor não numérico: {len(nao_numericos[campo])} registro(s)",
                "amostra": nao_numericos[campo][:8]
            }

        if is_numeric and campo in negativos and negativos[campo]:
            exemplos_negativos_ordenados = sorted(
                set([v for v in negativos[campo] if not is_vazio(v)]),
                key=lambda x: float(str(x).replace(',', '.')) if str(x).replace(',', '.').replace('.', '', 1).isdigit() else str(x)
            )
            inconsistencias[f'{campo}_negativo'] = {
                "label": label,
                "tipo": "negativo",
                "mensagem": f"Valor negativo: {len(negativos[campo])} registro(s)",
                "amostra": exemplos_negativos_ordenados[:8]
            }

        if campo in texto_ultrapassa and texto_ultrapassa[campo]:
            label_ = [x[1] for x in layout if x[0] == campo][0]
            inconsistencias[f'{campo}_ultrapassa'] = {
                "label": label_,
                "tipo": "ultrapassa_tamanho",
                "mensagem": f"Excede o limite de caracteres: {len(texto_ultrapassa[campo])} registro(s)",
                "amostra": sorted(
                    [v for v in texto_ultrapassa[campo] if not is_vazio(v)],
                    key=lambda x: (str(x).lower(), str(x))
                )[:8]
            }

        if em_branco_qtd > 0:
            inconsistencias[f'{campo}_em_branco'] = {
                "label": label,
                "tipo": "em_branco",
                "mensagem": f"Em branco: {em_branco_qtd} registro(s)",
                "amostra": []
            }

        if is_numeric and campo in zerados and zerados[campo]:
            exemplos_zerados_visiveis = [
                v for v in zerados[campo][:8]
                if str(v).replace('.', '').replace(',', '').strip('0') != ''
            ]
            inconsistencias[f'{campo}_zerado'] = {
                "label": label,
                "tipo": "zerado",
                "mensagem": f"Valor zerado: {len(zerados[campo])} registro(s)",
                "amostra": exemplos_zerados_visiveis
            }

    inconsistencias_ordenadas = dict(sorted(inconsistencias.items(), key=lambda x: x[1]['label'].lower()))
    total_validos_geral = sum(linha_valida)
    total_invalidos_geral = total_linhas - total_validos_geral

    return inconsistencias_ordenadas, stats, total_linhas, total_validos_geral, total_invalidos_geral


# ---------------- PESSOAS ---------------- #

def remover_acentos(txt):
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(txt))
        if unicodedata.category(c) != 'Mn'
    )

def normalizar(txt):
    return remover_acentos(str(txt)).strip().lower()

def normalizar_tipo_pessoa(valor):
    s = remover_acentos(str(valor)).strip().upper()
    if s in ["1", "PF", "F", "FISICA"]:
        return "1"
    if s in ["2", "PJ", "J", "JURIDICA"]:
        return "2"
    return s

def validar_opcoes(valor, opcoes):
    s = normalizar(valor)
    todas_opcoes = set()
    for nomes in opcoes.values():
        for n in nomes:
            todas_opcoes.add(normalizar(n))
    return s in todas_opcoes

def validar_campo_pessoas(campo, valor):
    if isinstance(valor, str):
        valor_limpo = valor.strip()
    else:
        valor_limpo = valor
    layout_info = next((item for item in LAYOUT_PESSOAS if item[0] == campo), None)
    if layout_info is None:
        return True
    tipo = layout_info[2].lower()
    tamanho = layout_info[3]
    obrigatorio = layout_info[4]

    # Obrigatoriedade
    if obrigatorio and (pd.isna(valor_limpo) or str(valor_limpo).strip() == ''):
        return False
    if not obrigatorio and (pd.isna(valor_limpo) or str(valor_limpo).strip() == ''):
        return True

    # Validações específicas

    if campo == "cpf_cnpj":
        s = str(valor_limpo).replace('.', '').replace('-', '').replace('/', '')
        return len(s) == 11 or len(s) == 14

    # Os campos abaixo já serão convertidos para número no DataFrame. Aqui só aceita número.
    if campo in ["tipo_pessoa", "tipo_contribuinte", "sexo", "estado_civil", "tipo_endereco", "tipo_telefone", "produtor_rural"]:
        return str(valor_limpo).strip() in [str(i) for i in range(1, 8)] + ['0', '9']

    # Tipo Numérico padrão
    if tipo == 'numérico':
        try:
            float(str(valor_limpo).replace(',', '.'))
            return True
        except:
            return False

    # Booleano padrão
    if tipo == 'booleano':
        return normalizar(valor_limpo) in ['1', '0', 'true', 'false', 'sim', 'nao', 'não', 'yes', 'no']

    # Data padrão
    if tipo == 'data':
        try:
            if not valor_limpo or valor_limpo == '':
                return True
            for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y'):
                try:
                    datetime.datetime.strptime(str(valor_limpo).strip(), fmt)
                    return True
                except:
                    continue
            return False
        except:
            return False

    # Texto padrão
    if tipo == 'texto':
        if tamanho and isinstance(tamanho, int) and len(str(valor_limpo)) > int(tamanho):
            return False
        return True
    return True

def analisar_dados_pessoas(df, layout, mapeamento):
    inconsistencias = {}
    stats = []
    total_linhas = len(df)
    linha_valida = [True] * total_linhas

    def is_vazio(v):
        if v is None: return True
        if isinstance(v, float):
            try:
                if math.isnan(v): return True
            except Exception: pass
        if isinstance(v, str):
            if v.strip() == "" or v.strip().lower() == "nan": return True
        try:
            if pd.isna(v): return True
        except Exception: pass
        return False

    # --- MAPS PARA TODOS OS CAMPOS DE OPÇÃO ---
    estado_civil_map = {
        'casado': '1', 'casado(a)': '1', '1': '1',
        'solteiro': '2', 'solteiro(a)': '2', '2': '2',
        'separado': '3', 'separado(a)': '3', '3': '3',
        'viuvo': '4', 'viúvo': '4', 'viuvo(a)': '4', 'viúvo(a)': '4', '4': '4',
        'desquitado': '5', 'desquitado(a)': '5', '5': '5',
        'divorciado': '6', 'divorciado(a)': '6', '6': '6',
        'outros': '7', 'outro': '7', 'outra': '7', '7': '7'
    }
    sexo_map = {
        'f': '1', 'feminino': '1', '1': '1',
        'm': '2', 'masculino': '2', '2': '2'
    }
    tipo_contribuinte_map = {
        'icms': '1', 'contribuinte': '1', '1': '1',
        'isento': '2', 'não contribuinte': '2', 'nao contribuinte': '2', '2': '2',
        '9': '9'
    }
    tipo_telefone_map = {
        'celular': '1', 'cel': '1', 'celular comercial': '1', '1': '1',
        'fixo': '2', 'residencial': '2', 'telefone fixo': '2', 'comercial': '2', '2': '2',
        'fax comercial': '3', '3': '3',
        'fax residencial': '4', '4': '4',
        'nextel': '5', '5': '5'
    }
    tipo_endereco_map = {
        'residencial': '1', '1': '1',
        'comercial': '2', '2': '2',
        'cobranca': '3', 'cobrança': '3', '3': '3',
        'secundario': '4', 'secundário': '4', '4': '4',
        'entrega': '5', '5': '5',
        'coleta': '6', '6': '6'
    }
    tipo_pessoa_map = {
        'pf': '1', 'f': '1', 'fisica': '1', 'física': '1', '1': '1',
        'pj': '2', 'j': '2', 'juridica': '2', 'jurídica': '2', '2': '2'
    }
    produtor_rural_map = {
        '1': '1', 'true': '1', 'sim': '1', 'yes': '1',
        '0': '0', 'false': '0', 'nao': '0', 'não': '0', 'no': '0'
    }

    # --- NORMALIZAÇÃO DOS CAMPOS DE OPÇÃO NO DATAFRAME (RESPEITA o mapeamento ATUAL) ---
    # Sempre sobrescreve as colunas do mapeamento atual, seja automático ou manual
    for campo, label, tipo, tamanho, obrigatorio in layout:
        col = mapeamento.get(campo)
        if not col or col not in df.columns:
            continue
        if campo == "tipo_telefone":
            df[col] = df[col].astype(str).apply(normalizar).replace(tipo_telefone_map)
        elif campo == "tipo_endereco":
            df[col] = df[col].astype(str).apply(normalizar).replace(tipo_endereco_map)
        elif campo == "tipo_pessoa":
            df[col] = df[col].astype(str).apply(normalizar).replace(tipo_pessoa_map)
        elif campo == "estado_civil":
            df[col] = df[col].astype(str).apply(normalizar).replace(estado_civil_map)
        elif campo == "sexo":
            df[col] = df[col].astype(str).apply(normalizar).replace(sexo_map)
        elif campo == "tipo_contribuinte":
            df[col] = df[col].astype(str).apply(normalizar).replace(tipo_contribuinte_map)
        elif campo == "produtor_rural":
            df[col] = df[col].astype(str).apply(normalizar).replace(produtor_rural_map)

    # --- VALIDAÇÃO PADRÃO ---
    for campo, label, tipo, tamanho, obrigatorio in layout:
        col = mapeamento.get(campo)
        if obrigatorio and (not col or col not in df.columns):
            inconsistencias[campo] = {
                "label": label,
                "tipo": "invalido",
                "mensagem": "Campo obrigatório não mapeado ou não encontrado.",
                "amostra": []
            }
            stats.append({'campo': label, 'validos': 0, 'invalidos': total_linhas})
            linha_valida = [False] * total_linhas
            continue
        if not col or col not in df.columns:
            stats.append({'campo': label, 'validos': 0, 'invalidos': 0})
            continue

        serie = df[col].astype(str)
        validos, invalidos = 0, 0
        em_branco_count = 0
        duplicados = set()
        fora_padrao = set()
        tamanho_maior = set()
        tamanho_menor = set()
        valor_invalido = set()
        # Para amostra, pode-se guardar até 5 valores de cada tipo se quiser

        # CPF/CNPJ
        if campo == "cpf_cnpj":
            valores_vistos = set()
            duplicados_cpf = set()
            caracteres_invalidos = set()
            em_branco_count = 0
            for idx, v in enumerate(serie):
                s_original = str(v).strip()
                if is_vazio(s_original):
                    invalidos += 1
                    em_branco_count += 1
                    linha_valida[idx] = False
                    continue
                if not re.fullmatch(r"[0-9.\-\/]+", s_original):
                    invalidos += 1
                    caracteres_invalidos.add(v)
                    linha_valida[idx] = False
                    continue
                s = s_original.replace('.', '').replace('-', '').replace('/', '')
                if not (len(s) == 11 or len(s) == 14):
                    invalidos += 1
                    fora_padrao.add(v)
                    linha_valida[idx] = False
                    continue
                if s in valores_vistos:
                    invalidos += 1
                    duplicados_cpf.add(v)
                    linha_valida[idx] = False
                    continue
                valores_vistos.add(s)
                validos += 1
            if em_branco_count:
                inconsistencias[f"{campo}_em_branco"] = {
                    "label": label,
                    "tipo": "em_branco",
                    "mensagem": f"Em branco: {em_branco_count} registro(s)",
                    "amostra": []
                }
            if caracteres_invalidos:
                inconsistencias[f"{campo}_caracteres_invalidos"] = {
                    "label": label,
                    "tipo": "caracteres_invalidos",
                    "mensagem": f"Caracteres inválidos: {len(caracteres_invalidos)} registro(s)",
                    "amostra": sorted(caracteres_invalidos)
                }
            if duplicados_cpf:
                inconsistencias[f"{campo}_duplicado"] = {
                    "label": label,
                    "tipo": "duplicado",
                    "mensagem": f"Duplicados: {len(duplicados_cpf)} registro(s)",
                    "amostra": sorted(duplicados_cpf)
                }
            if fora_padrao:
                inconsistencias[campo] = {
                    "label": label,
                    "tipo": "invalido",
                    "mensagem": f"Fora do padrão (deve ter 11 ou 14 dígitos): {len(fora_padrao)} registro(s)",
                    "amostra": sorted(fora_padrao)
                }
            stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})
            continue

        # E-mail
        if campo == "email":
            em_branco_count = 0
            for idx, v in enumerate(serie):
                s = str(v).strip()
                if is_vazio(s):
                    if obrigatorio:
                        invalidos += 1
                        em_branco_count += 1
                        linha_valida[idx] = False
                    continue
                if re.search(r"\s", s) or re.search(r"[çãõáéíóúâêîôûàèìòùäëïöü]", s, re.IGNORECASE):
                    invalidos += 1
                    valor_invalido.add(v)
                    linha_valida[idx] = False
                    continue
                if not re.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", s):
                    invalidos += 1
                    fora_padrao.add(v)
                    linha_valida[idx] = False
                    continue
                validos += 1
            if em_branco_count:
                inconsistencias[f"{campo}_em_branco"] = {
                    "label": label,
                    "tipo": "em_branco",
                    "mensagem": f"Em branco: {em_branco_count} registro(s)",
                    "amostra": []
                }
            if valor_invalido:
                inconsistencias[f"{campo}_caractere_invalido"] = {
                    "label": label,
                    "tipo": "caractere_invalido",
                    "mensagem": "Contém espaço ou caractere especial inválido",
                    "amostra": sorted(valor_invalido)
                }
            if fora_padrao:
                inconsistencias[campo] = {
                    "label": label,
                    "tipo": "invalido",
                    "mensagem": "Fora do padrão de e-mail",
                    "amostra": sorted(fora_padrao)
                }
            stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})
            continue

        #CEP
        if campo == "cep":
            em_branco_count = 0
            for idx, v in enumerate(serie):
                s = str(v).strip()
                if is_vazio(s):
                    if obrigatorio:
                        invalidos += 1
                        em_branco_count += 1
                        linha_valida[idx] = False
                    continue
                s_limpo = s.replace("-", "")
                if len(s) < 8 or len(s) > 9:
                    invalidos += 1
                    fora_padrao.add(v)
                    linha_valida[idx] = False
                    continue
                if not re.match(r"^\d{5}-?\d{3}$", s):
                    invalidos += 1
                    valor_invalido.add(v)
                    linha_valida[idx] = False
                    continue
                validos += 1
            if em_branco_count:
                inconsistencias[f"{campo}_em_branco"] = {
                    "label": label,
                    "tipo": "em_branco",
                    "mensagem": f"Em branco: {em_branco_count} registro(s)",
                    "amostra": []
                }
            if fora_padrao:
                inconsistencias[f"{campo}_tamanho_invalido"] = {
                    "label": label,
                    "tipo": "tamanho_invalido",
                    "mensagem": f"Tamanho de caracteres inválido.  Total: {len(fora_padrao)} registro(s).",
                    "amostra": sorted(fora_padrao)
                }
            if valor_invalido:
                inconsistencias[campo] = {
                    "label": label,
                    "tipo": "invalido",
                    "mensagem": f"Contém caracteres não numéricos ou hífen fora do lugar. Total: {len(valor_invalido)} registro(s).",
                    "amostra": sorted(valor_invalido)
                }
            stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})
            continue

        # Texto (logradouro, número endereço, etc)
        if tipo.lower() == "texto":
            em_branco_count = 0
            for idx, v in enumerate(serie):
                s = str(v)
                if is_vazio(s):
                    if obrigatorio:
                        invalidos += 1
                        em_branco_count += 1
                        linha_valida[idx] = False
                    continue
                if tamanho and len(s) > int(tamanho):
                    invalidos += 1
                    tamanho_maior.add(v)
                    linha_valida[idx] = False
                    continue
                if tamanho and len(s) < 1:
                    invalidos += 1
                    tamanho_menor.add(v)
                    linha_valida[idx] = False
                    continue
                validos += 1
            if em_branco_count:
                inconsistencias[f"{campo}_em_branco"] = {
                    "label": label,
                    "tipo": "em_branco",
                    "mensagem": f"Em branco: {em_branco_count} registro(s)",
                    "amostra": []
                }
            if tamanho_maior or tamanho_menor:
                desc = []
                if tamanho_maior: desc.append("Excede o limite de caracteres")
                if tamanho_menor: desc.append("Abaixo do limite de caracteres")
                inconsistencias[campo] = {
                    "label": label,
                    "tipo": "invalido",
                    "mensagem": "; ".join(desc) + f". Total: {len(tamanho_maior | tamanho_menor)} registro(s).",
                    "amostra": sorted(tamanho_maior | tamanho_menor)
                }
            stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})
            continue

        # Numérico
        if tipo.lower() == "numérico":
            em_branco_count = 0
            for idx, v in enumerate(serie):
                s = str(v).replace(',', '.')
                if is_vazio(s):
                    if obrigatorio:
                        invalidos += 1
                        em_branco_count += 1
                        linha_valida[idx] = False
                    continue
                try:
                    float(s)
                    validos += 1
                except:
                    invalidos += 1
                    valor_invalido.add(v)
                    linha_valida[idx] = False
            if em_branco_count:
                inconsistencias[f"{campo}_em_branco"] = {
                    "label": label,
                    "tipo": "em_branco",
                    "mensagem": f"Em branco: {em_branco_count} registro(s)",
                    "amostra": []
                }
            if valor_invalido:
                inconsistencias[campo] = {
                    "label": label,
                    "tipo": "invalido",
                    "mensagem": f"Valor não numérico. Total: {len(valor_invalido)} registro(s).",
                    "amostra": sorted(valor_invalido)
                }
            stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})
            continue

        # Booleano
        if tipo.lower() == "booleano":
            em_branco_count = 0
            for idx, v in enumerate(serie):
                if is_vazio(v):
                    if obrigatorio:
                        invalidos += 1
                        em_branco_count += 1
                        linha_valida[idx] = False
                    continue
                if not normalizar(v) in ['1', '0', 'true', 'false', 'sim', 'nao', 'não', 'yes', 'no']:
                    invalidos += 1
                    valor_invalido.add(v)
                    linha_valida[idx] = False
                    continue
                validos += 1
            if em_branco_count:
                inconsistencias[f"{campo}_em_branco"] = {
                    "label": label,
                    "tipo": "em_branco",
                    "mensagem": f"Em branco: {em_branco_count} registro(s)",
                    "amostra": []
                }
            if valor_invalido:
                inconsistencias[campo] = {
                    "label": label,
                    "tipo": "invalido",
                    "mensagem": f"Valor fora do padrão booleano (1/0/Sim/Não/True/False). Total: {len(valor_invalido)} registro(s).",
                    "amostra": sorted(valor_invalido)
                }
            stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})
            continue

        # Data
        if tipo.lower() == "data":
            em_branco_count = 0
            for idx, v in enumerate(serie):
                s = str(v).strip()
                if is_vazio(s):
                    if obrigatorio:
                        invalidos += 1
                        em_branco_count += 1
                        linha_valida[idx] = False
                    continue
                ok = False
                for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y'):
                    try:
                        datetime.datetime.strptime(s, fmt)
                        ok = True
                        break
                    except:
                        continue
                if not ok:
                    invalidos += 1
                    valor_invalido.add(v)
                    linha_valida[idx] = False
                else:
                    validos += 1
            if em_branco_count:
                inconsistencias[f"{campo}_em_branco"] = {
                    "label": label,
                    "tipo": "em_branco",
                    "mensagem": f"Em branco: {em_branco_count} registro(s)",
                    "amostra": []
                }
            if valor_invalido:
                inconsistencias[campo] = {
                    "label": label,
                    "tipo": "invalido",
                    "mensagem": f"Valor fora do padrão de data esperado (ex: dd/mm/aaaa ou yyyy-mm-dd). Total: {len(valor_invalido)} registro(s).",
                    "amostra": sorted(valor_invalido)
                }
            stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})
            continue

    inconsistencias_ordenadas = dict(sorted(inconsistencias.items(), key=lambda x: x[1]['label'].lower()))
    total_validos_geral = sum(linha_valida)
    total_invalidos_geral = total_linhas - total_validos_geral

    return inconsistencias_ordenadas, stats, total_linhas, total_validos_geral, total_invalidos_geral

def auto_map_header_pessoas(df, layout, keywords):
    auto_map = {}
    for campo, *_ in layout:
        melhor_col, melhor_score = None, 0
        for col in df.columns:
            col_norm = normalizar_nome(col)
            for key in keywords.get(campo, [campo]):
                key_norm = normalizar_nome(key)
                score = similaridade(col_norm, key_norm)
                if score > melhor_score and score >= 0.82:
                    melhor_col, melhor_score = col, score
        if melhor_col:
            auto_map[campo] = melhor_col
    return auto_map

def auto_map_by_data_pessoas(df, layout, mapping_history):
    auto_map = {}
    for campo, *_ in layout:
        if campo in mapping_history and mapping_history[campo].get("amostras_validas"):
            campo_amostras = set(mapping_history[campo]["amostras_validas"])
            melhor_col, melhor_score = None, 0
            for col in df.columns:
                serie = df[col].dropna().astype(str)
                serie_validas = set([v for v in serie if validar_campo_pessoas(campo, v)])
                if not serie_validas:
                    continue
                intersecao = campo_amostras.intersection(serie_validas)
                score = len(intersecao) / max(len(serie_validas), 1)
                if score > melhor_score and score >= 0.5:
                    melhor_col, melhor_score = col, score
            if melhor_col:
                auto_map[campo] = melhor_col
    return auto_map

def aprender_metadados_coluna_pessoas(serie, campo_layout, old_samples=None):
    valid_samples = set(s for s in (old_samples or []) if s != "")
    for val in pd.Series(serie.dropna().astype(str).unique()):
        if validar_campo_pessoas(campo_layout, val) and val != "":
            valid_samples.add(val)
    return list(valid_samples)


# --------------- VEÍCULO DO CLIENTE --------------- #

def validar_campo_veiculos_cliente(campo, valor):
    if isinstance(valor, str):
        valor_limpo = valor.strip()
    else:
        valor_limpo = valor

    layout_info = next((item for item in LAYOUT_VEICULO_CLIENTE if item[0] == campo), None)
    if layout_info and layout_info[4] and (pd.isna(valor_limpo) or str(valor_limpo).strip() == ''):
        return False
    if layout_info and not layout_info[4] and (pd.isna(valor_limpo) or str(valor_limpo).strip() == ''):
        return True
    max_len = layout_info[3] if layout_info and isinstance(layout_info[3], int) else float('inf')

    if campo == 'cpf_cnpj':
        s = str(valor_limpo).strip()
        return s.isdigit() and len(s) == 11 or len(s) == 14

    if campo == 'placa':
        # Placa padrão Brasil: 3 letras + 4 números ou Mercosul: 3 letras + 1 número + 1 letra + 2 números
        s = str(valor_limpo).strip().upper().replace("-", "")
        return len(s) == 7 or len(s) == 8

    if campo == 'modelo':
        return isinstance(valor_limpo, str) and 1 <= len(str(valor_limpo)) <= max_len

    if campo == 'cor':
        return isinstance(valor_limpo, str) and 1 <= len(str(valor_limpo)) <= max_len

    if campo in ['ano_fabricacao', 'ano_modelo']:
        s = str(valor_limpo).strip()
        return s.isdigit() and len(s) == 4 and 1900 <= int(s) <= 2100

    if campo == 'chassi':
        # Chassi geralmente 17, mas layout permite até 20
        s = str(valor_limpo).strip().upper()
        return 1 <= len(s) <= max_len

    if campo in ['motor', 'renavam', 'crlv', 'bateria', 'valor_bem', 'revendedora', 'codigo_revendedora', 'ultima_concessionaria_exec',
                 'numero_produtor_rural', 'id_estrangeiro', 'inscricao_estadual']:
        return isinstance(valor_limpo, str) and len(str(valor_limpo).strip()) <= max_len

    if campo in ['rg']:
        return isinstance(valor_limpo, str) and len(str(valor_limpo).strip()) <= max_len

    if campo in ['uf_rg', 'uf_inscricao_estadual']:
        s = str(valor_limpo).strip()
        return s.isdigit() and len(s) == 2

    if campo in ['data_venda', 'data_inicial_garantia', 'data_final_garantia']:
        s = str(valor_limpo).strip()
        # Espera formato dd/mm/yyyy ou yyyy-mm-dd
        if not s:
            return True
        for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y'):
            try:
                datetime.datetime.strptime(s, fmt)
                return True
            except Exception:
                continue
        return False

    if campo == 'data_hora_ultima_alteracao':
        s = str(valor_limpo).strip()
        # Espera formato yyyy-mm-dd HH:MM:SS ou similar
        if not s:
            return True
        # Aceita ISO ou dd/mm/yyyy HH:MM:SS
        for fmt in ('%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'):
            try:
                datetime.datetime.strptime(s, fmt)
                return True
            except Exception:
                continue
        return False

    return True

def analisar_dados_veiculos_cliente(df, layout, mapeamento):
    inconsistencias = {}
    stats = []
    total_linhas = len(df)
    linha_valida = [True] * total_linhas

    def is_vazio(v):
        if v is None:
            return True
        if isinstance(v, float):
            try:
                if math.isnan(v):
                    return True
            except Exception:
                pass
        if isinstance(v, str):
            if v.strip() == "" or v.strip().lower() == "nan":
                return True
        try:
            if pd.isna(v):
                return True
        except Exception:
            pass
        return False

    # Exemplo de verificação: duplicidade de chassi, placa, cpf_cnpj
    chassi_col = mapeamento.get('chassi')
    chassi_vistos = set()
    chassi_duplicados = set()
    exemplos_chassi_duplicados = []

    placa_col = mapeamento.get('placa')
    placa_vistos = set()
    placa_duplicados = set()
    exemplos_placa_duplicados = []

    if chassi_col and chassi_col in df.columns:
        for idx, v in enumerate(df[chassi_col]):
            if is_vazio(v):
                continue
            valor = str(v).strip().upper()
            if valor in chassi_vistos:
                chassi_duplicados.add(valor)
                if len(exemplos_chassi_duplicados) < 8:
                    exemplos_chassi_duplicados.append(valor)
                linha_valida[idx] = False
            else:
                chassi_vistos.add(valor)

    if placa_col and placa_col in df.columns:
        for idx, v in enumerate(df[placa_col]):
            if is_vazio(v):
                continue
            valor = str(v).strip().upper()
            if valor in placa_vistos:
                placa_duplicados.add(valor)
                if len(exemplos_placa_duplicados) < 8:
                    exemplos_placa_duplicados.append(valor)
                linha_valida[idx] = False
            else:
                placa_vistos.add(valor)

    if chassi_duplicados:
        inconsistencias['chassi_duplicado'] = {
            "label": "Chassi",
            "tipo": "duplicado",
            "mensagem": f"Duplicados: {len(chassi_duplicados)} registro(s)",
            "amostra": exemplos_chassi_duplicados
        }
    if placa_duplicados:
        inconsistencias['placa_duplicado'] = {
            "label": "Placa",
            "tipo": "duplicado",
            "mensagem": f"Duplicados: {len(placa_duplicados)} registro(s)",
            "amostra": exemplos_placa_duplicados
        }

    texto_ultrapassa = {}
    texto_nao_texto = {}

    for campo, label, tipo, tamanho, obrigatorio in layout:
        col = mapeamento.get(campo)
        if obrigatorio and (not col or col not in df.columns):
            inconsistencias[campo] = {
                "label": label,
                "tipo": "invalido",
                "mensagem": "Campo obrigatório não mapeado ou não encontrado.",
                "amostra": []
            }
            stats.append({'campo': label, 'validos': 0, 'invalidos': total_linhas})
            linha_valida = [False] * total_linhas
            continue
        if not col or col not in df.columns:
            stats.append({'campo': label, 'validos': 0, 'invalidos': 0})
            continue

        serie = df[col].astype(str)
        validos, invalidos = 0, 0
        em_branco_qtd = 0
        exemplos_em_branco = []
        exemplos_outros_invalidos = []

        for idx, v in enumerate(serie):
            vazio = is_vazio(v)
            if tipo.lower() == 'texto':
                maxlen = [x[3] for x in layout if x[0] == campo][0]
                if not isinstance(v, str):
                    texto_nao_texto.setdefault(campo, set()).add(v)
                if maxlen and isinstance(v, str) and v.strip() != "" and len(str(v)) > int(maxlen):
                    texto_ultrapassa.setdefault(campo, set()).add(v)
            valido = validar_campo_veiculos_cliente(campo, v)
            if not valido:
                invalidos += 1
                if vazio:
                    em_branco_qtd += 1
                    if len(exemplos_em_branco) < 8:
                        exemplos_em_branco.append(v)
                else:
                    if len(exemplos_outros_invalidos) < 8 and not is_vazio(v):
                        exemplos_outros_invalidos.append(v)
                if obrigatorio:
                    linha_valida[idx] = False
            else:
                validos += 1

        stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})

        if invalidos - em_branco_qtd > 0:
            exemplos_distintos_ordenados = sorted(
                set([v for v in exemplos_outros_invalidos if not is_vazio(v)]),
                key=lambda x: (str(x).lower(), str(x))
            )
            inconsistencias[campo] = {
                "label": label,
                "tipo": "invalido",
                "mensagem": f"Valor inválido: {invalidos - em_branco_qtd} registro(s)",
                "amostra": exemplos_distintos_ordenados
            }

        if em_branco_qtd > 0:
            inconsistencias[f'{campo}_em_branco'] = {
                "label": label,
                "tipo": "em_branco",
                "mensagem": f"Em branco: {em_branco_qtd} registro(s)",
                "amostra": []
            }

    for campo, valores in texto_ultrapassa.items():
        label = [x[1] for x in layout if x[0] == campo][0]
        inconsistencias[f'{campo}_ultrapassa'] = {
            "label": label,
            "tipo": "ultrapassa_tamanho",
            "mensagem": f"Excede o limite de caracteres: {len(valores)} registro(s)",
            "amostra": sorted(
                [v for v in valores if not is_vazio(v)],
                key=lambda x: (str(x).lower(), str(x))
            )
        }

    for campo, valores in texto_nao_texto.items():
        label = [x[1] for x in layout if x[0] == campo][0]
        inconsistencias[f'{campo}_nao_texto'] = {
            "label": label,
            "tipo": "nao_texto",
            "mensagem": f"Valor não é texto: {len(valores)} registro(s)",
            "amostra": sorted(
                [v for v in valores if not is_vazio(v)],
                key=lambda x: (str(x).lower(), str(x))
            )
        }

    inconsistencias_ordenadas = dict(sorted(inconsistencias.items(), key=lambda x: x[1]['label'].lower()))
    total_validos_geral = sum(linha_valida)
    total_invalidos_geral = total_linhas - total_validos_geral

    return inconsistencias_ordenadas, stats, total_linhas, total_validos_geral, total_invalidos_geral

def auto_map_header_veiculos_cliente(df, layout, keywords):
    auto_map = {}
    for campo, *_ in layout:
        melhor_col, melhor_score = None, 0
        for col in df.columns:
            col_norm = normalizar_nome(col)
            for key in keywords.get(campo, [campo]):
                key_norm = normalizar_nome(key)
                score = similaridade(col_norm, key_norm)
                if score > melhor_score and score >= 0.82:
                    melhor_col, melhor_score = col, score
        if melhor_col:
            auto_map[campo] = melhor_col
    return auto_map


def auto_map_by_data_veiculos_cliente(df, layout, mapping_history):
    auto_map = {}
    for campo, *_ in layout:
        if campo in mapping_history and mapping_history[campo].get("amostras_validas"):
            campo_amostras = set(mapping_history[campo]["amostras_validas"])
            melhor_col, melhor_score = None, 0
            for col in df.columns:
                serie = df[col].dropna().astype(str)
                serie_validas = set([v for v in serie if validar_campo_veiculos_cliente(campo, v)])
                if not serie_validas:
                    continue
                intersecao = campo_amostras.intersection(serie_validas)
                score = len(intersecao) / max(len(serie_validas), 1)
                if score > melhor_score and score >= 0.5:
                    melhor_col, melhor_score = col, score
            if melhor_col:
                auto_map[campo] = melhor_col
    return auto_map


def aprender_metadados_coluna_veiculos_cliente(serie, campo_layout, old_samples=None):
    valid_samples = set(s for s in (old_samples or []) if s != "")
    for val in pd.Series(serie.dropna().astype(str).unique()):
        if validar_campo_veiculos_cliente(campo_layout, val) and val != "":
            valid_samples.add(val)
    return list(valid_samples)



# ------------ R O T A S  ------------ #

@app.route('/')
def index():
    return redirect(url_for('principal'))

@app.route('/principal')
def principal():
    return render_template("principal.html")

@app.route('/validador/<tipo>', methods=['GET'])
def validador(tipo):
    if tipo not in LAYOUTS:
        abort(404)
    contexto = LAYOUTS[tipo]
    mapear = session.get('mapear', None)
    inconsistencias = session.get('inconsistencias', None)
    stats = session.get('stats', None)
    total_registros = session.get('total_registros', 0)
    return render_template(
        "validador.html",
        tipo=tipo,
        nome_rotina=contexto["nome"],
        layout=contexto["layout"],
        js_file=contexto["js"],
        mapear=mapear,
        inconsistencias=inconsistencias,
        stats=stats,
        total_registros=total_registros
    )

@app.route('/validador/<tipo>/upload', methods=['POST'])
def validador_upload(tipo):
    if tipo not in LAYOUTS:
        abort(404)
    tipo_layout = tipo
    contexto = LAYOUTS[tipo]
    layout = contexto["layout"]
    keywords = contexto["keywords"]
    session['dataframes'] = {}
    arquivos_para_mapear = []
    mapping_history = load_mapping_history(tipo_layout)
    total_registros = 0
    alerta_quebra = []

    arquivos = request.files.getlist('files')
    auto_map_header_func = globals()[f'auto_map_header_{tipo}']
    auto_map_by_data_func = globals()[f'auto_map_by_data_{tipo}']

    for file in arquivos:
        if not file:
            continue
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        ext = os.path.splitext(filename)[1].lower()
        df = None
        try:
            if ext in ['.csv', '.txt']:
                with open(filepath, 'rb') as f_bytes:
                    raw = f_bytes.read()
                df, sep, encoding, linhas_ignoradas_indices = detectar_encoding_e_linhas_validas(raw, extensao=ext, filename=filename)
                if linhas_ignoradas_indices:
                    alerta_quebra.extend(linhas_ignoradas_indices)
            elif ext == '.xlsx':
                with open(filepath, 'rb') as f_bytes:
                    raw = f_bytes.read()
                df, sep, encoding, linhas_ignoradas_indices = detectar_encoding_e_linhas_validas(raw, extensao=ext, filename=filename)
                if linhas_ignoradas_indices:
                    alerta_quebra.extend(linhas_ignoradas_indices)
            else:
                arquivos_para_mapear.append({'nome': filename, 'erro': 'Formato não suportado.'})
                continue

            num_registros = len(df)
            total_registros += num_registros
            auto_map = auto_map_header_func(df, layout, keywords)
            obrigatorios = [c for c, _, _, _, o in layout if o]

            if not all(auto_map.get(c) for c in obrigatorios):
                auto_map_data = auto_map_by_data_func(df, layout, mapping_history)
                for campo, col in auto_map_data.items():
                    if campo not in auto_map:
                        auto_map[campo] = col

            pedir_manual = not all(auto_map.get(c) for c in obrigatorios)
            session['dataframes'][filename] = df.to_json(orient='split')
            arquivos_para_mapear.append({
                'nome': filename,
                'colunas': df.columns.tolist(),
                'amostra': df.head(20).where(pd.notnull(df.head(20)), '').to_dict('records'),
                'auto_map': auto_map,
                'has_header': True,
                'pedir_manual': pedir_manual,
                'num_registros': num_registros,
            })
        except Exception as e:
            arquivos_para_mapear.append({'nome': filename, 'erro': f'Erro: {str(e)}'})
            if os.path.exists(filepath):
                os.remove(filepath)

    session['mapear'] = arquivos_para_mapear
    session['tipo_layout'] = tipo_layout
    session['total_registros'] = total_registros
    if alerta_quebra:
        session['alerta_quebra'] = "\n".join(alerta_quebra)
    else:
        session.pop('alerta_quebra', None)
    return redirect(url_for('validador', tipo=tipo))

@app.route('/validador/<tipo>/mapear', methods=['POST'])
def validador_mapear(tipo):
    if tipo not in LAYOUTS:
        abort(404)

    tipo_layout = tipo
    layout = LAYOUTS[tipo_layout]["layout"]
    analyzer = globals()[f'analisar_dados_{tipo}']
    validator = globals()[f'validar_campo_{tipo}']
    mapear = session.get('mapear', [])
    dataframes = session.get('dataframes', {})

    # Carrega o histórico existente para comparar
    mapping_history = load_mapping_history(tipo_layout)
    # Dicionário para guardar apenas as amostras novas
    history_para_salvar = {}

    novos_arquivos = []
    stats_totais = []

    for item_data in mapear:
        nome_arquivo = item_data['nome']
        if nome_arquivo not in dataframes:
            continue

        df = pd.read_json(io.StringIO(dataframes[nome_arquivo]), orient='split')
        df = normalizar_colunas_vazias(df)

        mapeamento_do_usuario = {campo[0]: request.form.get(f"{nome_arquivo}_{campo[0]}") for campo in layout}
        mapeamento_do_usuario = {k: v for k, v in mapeamento_do_usuario.items() if v}

        for campo, col_name in mapeamento_do_usuario.items():
            if col_name in df.columns:
                serie = df[col_name]
                old_samples_set = set(mapping_history.get(campo, {}).get("amostras_validas", []))

                # Valida e coleta todas as amostras únicas do arquivo atual
                all_valid_from_serie = {str(v) for v in serie.dropna().unique() if validator(campo, v) and str(v)}

                # Identifica apenas as amostras que são genuinamente novas
                novas_amostras = list(all_valid_from_serie - old_samples_set)

                if novas_amostras:
                    history_para_salvar[campo] = {"amostras_validas": novas_amostras}

        inconsistencias, stats, total_linhas, total_validos_geral, total_invalidos_geral = analyzer(df, layout, mapeamento_do_usuario)

        novos_arquivos.append({
            'nome': nome_arquivo, 'mapeamento': mapeamento_do_usuario, 'inconsistencias': inconsistencias
        })
        stats_totais.append({
            'nome': nome_arquivo, 'stats': stats, 'total_registros': total_linhas,
            'total_validos_geral': total_validos_geral, 'total_invalidos_geral': total_invalidos_geral
        })

    if history_para_salvar:
        save_mapping_history(tipo_layout, history_para_salvar)

    session['inconsistencias'] = novos_arquivos
    session['stats'] = stats_totais
    session.pop('dataframes', None)
    session.pop('mapear', None)
    session.pop('tipo_layout', None)
    return redirect(url_for('validador', tipo=tipo))

@app.template_filter('reais')
def reais_format(valor):
    try:
        valor = float(valor)
        return f"R$ {valor:,.2f}".replace(',', 'v').replace('.', ',').replace('v', '.')
    except Exception:
        return f"R$ {valor}"

@app.route('/validador/<tipo>/reset', methods=['POST'])
def validador_reset(tipo):
    session.clear()
    limpar_uploads()
    return redirect(url_for('principal'))

# ----------- HISTORY ------------------

@app.route('/get_amostra', methods=['POST'])
def get_amostra():
    nome_arquivo = request.form.get('nome_arquivo')
    offset = int(request.form.get('offset', 0))
    limit = int(request.form.get('limit', 20))
    if 'dataframes' not in session or nome_arquivo not in session['dataframes']:
        return jsonify({'ok': False, 'erro': 'Arquivo não encontrado na sessão.'})
    df = pd.read_json(io.StringIO(session['dataframes'][nome_arquivo]), orient='split')
    df = normalizar_colunas_vazias(df)
    df_amostra = df.iloc[offset:offset+limit]
    amostra = df_amostra.where(pd.notnull(df_amostra), '').to_dict('records')
    return jsonify({'ok': True, 'amostra': amostra, 'colunas': list(df.columns)})

@app.route('/history_ia', methods=['GET'])
def mapping_history_ia():
    token = request.args.get('token', '')
    if token != 'ia-secrect':
        return "Acesso restrito.", 403

    amostras_limit = 50
    mensagem = session.pop('mensagem', None)
    history_por_layout = {name: {"nome": config["nome"], "campos": {}} for name, config in LAYOUTS.items()}
    conn = get_db()

    if conn:
        try:
            with conn.cursor() as cur:
                for layout_name in LAYOUTS.keys():
                    table_name = layout_name
                    query = f"""
                        SELECT field_name, COUNT(id) as total,
                               (array_agg(sample_value))[1:%s] as amostras
                        FROM {table_name}
                        GROUP BY field_name
                        ORDER BY field_name;
                    """
                    cur.execute(query, (amostras_limit,))
                    results = cur.fetchall()
                    for row in results:
                        field_name, total, amostras = row
                        history_por_layout[layout_name]["campos"][field_name] = {
                            "nome": field_name,
                            "amostras": amostras or [],
                            "total": total
                        }
        except Exception as e:
            print(f"Erro ao carregar histórico: {e}")

    return render_template(
        "history_ia.html",
        history_por_layout=history_por_layout,
        mensagem=mensagem,
        amostras_limit=amostras_limit
    )

@app.route('/history_ia/busca_amostras', methods=['POST'])
def history_ia_busca_amostras():
    token = request.form.get('token', '')
    if token != 'ia-secrect':
        return jsonify({"error": "Acesso restrito."}), 403

    layout = request.form.get('layout')
    campo = request.form.get('campo')
    termo = request.form.get('termo', '').lower()
    limit = int(request.form.get('limit', 100))
    table_name = layout
    resultados = []
    total = 0
    conn = get_db()

    if conn:
        try:
            with conn.cursor() as cur:
                query = f"""
                    SELECT sample_value FROM {table_name}
                    WHERE field_name = %s AND sample_value ILIKE %s
                    LIMIT %s;
                """
                cur.execute(query, (campo, f'%{termo}%', limit))
                resultados = [row[0] for row in cur.fetchall()]

                count_query = f"SELECT COUNT(id) FROM {table_name} WHERE field_name = %s AND sample_value ILIKE %s;"
                cur.execute(count_query, (campo, f'%{termo}%'))
                total = cur.fetchone()[0]
        except Exception as e:
            print(f"Erro na busca em '{table_name}': {e}")

    return jsonify(amostras=resultados, total=total)

@app.route('/history_ia/delete', methods=['POST'])
def history_ia_delete():
    token = request.form.get('token', '')
    if token != 'ia-secrect':
        return jsonify({"success": False, "mensagem": "Acesso restrito."}), 403

    layout = request.form.get('layout')
    campo = request.form.get('campo')
    valor = request.form.get('valor')
    acao = request.form.get('acao')
    table_name = layout
    success = False
    mensagem = ""
    conn = get_db()

    if conn:
        try:
            with conn.cursor() as cur:
                if acao == 'delcampo':
                    query = f"DELETE FROM {table_name} WHERE field_name = %s;"
                    cur.execute(query, (campo,))
                    mensagem = f"Todas as amostras do campo '{campo}' foram removidas."
                    success = True
                elif acao == 'delvalor' and valor is not None:
                    query = f"DELETE FROM {table_name} WHERE field_name = %s AND sample_value = %s;"
                    cur.execute(query, (campo, valor))
                    if cur.rowcount > 0:
                        mensagem = f'Valor "{valor}" removido do campo "{campo}".'
                        success = True
                    else:
                        mensagem = f'Valor "{valor}" não encontrado.'
            conn.commit()
        except Exception as e:
            conn.rollback()
            mensagem = f"Erro ao excluir de '{table_name}': {e}"
    else:
        mensagem = "Não foi possível conectar ao banco de dados."

    return jsonify({"success": success, "mensagem": mensagem})

if __name__ == '__main__':
    app.run(debug=True)