from flask import Flask, render_template, request, redirect, url_for, session, jsonify, abort, g, flash, send_from_directory
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
from decimal import Decimal

from models.database import db, init_db as init_database, Organizacao, CNPJ, SessaoMigracao, ProgressoMigracao
from services import file_processor, mapping_service, validation_service
from modules.mercadorias import MercadoriasProcessor
from modules.pessoas import PessoasProcessor
from modules.veiculocliente import VeiculoClienteProcessor
# Placeholder for other processors
# from modules.mercadorias_saldos import MercadoriasSaldosProcessor

app = Flask(__name__)
app.config.from_object('config')
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

Session(app)

# Initialize SQLAlchemy and create new tables
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{app.config['DB_USER']}:{app.config['DB_PASS']}@{app.config['DB_HOST']}/{app.config['DB_NAME']}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
init_database(app)

# Processor mapping
PROCESSORS = {
    "mercadorias": MercadoriasProcessor,
    "pessoas": PessoasProcessor,
    "veiculos_cliente": VeiculoClienteProcessor,
    # "mercadorias_saldos": MercadoriasSaldosProcessor, # Placeholder
}

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

def init_history_db():
    """Cria as tabelas de histórico da IA se elas não existirem."""
    conn = get_db()
    if conn is None:
        print("ERRO: Não foi possível conectar ao banco de dados PostgreSQL para o histórico da IA.")
        return

    try:
        with conn.cursor() as cur:
            # Cria uma tabela para cada layout para o histórico da IA
            for layout_name in LAYOUTS.keys():
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
        print(f"Erro ao inicializar o banco de dados de histórico: {e}")
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
    init_history_db()

# ... (rest of the file remains the same, so it's omitted for brevity)
# I will just copy the rest of the original file content here


# ------------ NEW APP FLOW ROUTES ------------ #

@app.route('/')
def index():
    """Displays the organization selection screen."""
    organizacoes = Organizacao.query.order_by(Organizacao.nome).all()
    return render_template('selecionar_organizacao.html', organizacoes=organizacoes)

@app.route('/dashboard')
def dashboard():
    """Redirects to the new main page."""
    return redirect(url_for('index'))

@app.route('/selecionar-layout/<int:organizacao_id>')
def selecionar_layout(organizacao_id):
    """Displays the layout selection screen for a given organization."""
    organizacao = Organizacao.query.get_or_404(organizacao_id)
    return render_template('selecionar_layout.html', organizacao=organizacao, layouts=LAYOUTS)

@app.route('/organizacao/adicionar', methods=['GET', 'POST'])
def adicionar_organizacao():
    """Handles the creation of a new organization with its matrix and branch CNPJs."""
    if request.method == 'POST':
        nome = request.form.get('nome')
        codigo = request.form.get('codigo')
        cnpj_matriz = request.form.get('cnpj_matriz')
        cnpjs_filiais = request.form.getlist('cnpjs_filiais[]')

        if not nome or not codigo or not cnpj_matriz:
            flash('Nome, Código e CNPJ da Matriz são campos obrigatórios.', 'danger')
            return redirect(url_for('adicionar_organizacao'))

        if Organizacao.query.filter_by(codigo=codigo).first():
            flash(f'O código "{codigo}" já está em uso.', 'danger')
            return redirect(url_for('adicionar_organizacao'))

        # Check for duplicate CNPJs
        todos_cnpjs = [cnpj_matriz] + [cnpj for cnpj in cnpjs_filiais if cnpj]
        if CNPJ.query.filter(CNPJ.numero.in_(todos_cnpjs)).first():
            flash('Um ou mais CNPJs informados já estão cadastrados no sistema.', 'danger')
            return redirect(url_for('adicionar_organizacao'))

        # Create Organization and CNPJs
        nova_organizacao = Organizacao(nome=nome, codigo=codigo)
        db.session.add(nova_organizacao)

        matriz = CNPJ(numero=cnpj_matriz, is_matriz=True, organizacao=nova_organizacao)
        db.session.add(matriz)

        for cnpj_filial in cnpjs_filiais:
            if cnpj_filial: # Ensure it's not an empty string
                filial = CNPJ(numero=cnpj_filial, is_matriz=False, organizacao=nova_organizacao)
                db.session.add(filial)

        db.session.flush() # Flush to assign nova_organizacao.id

        # Create initial progress entries
        for modulo_key in LAYOUTS.keys():
            progresso = ProgressoMigracao(organizacao_id=nova_organizacao.id, modulo=modulo_key)
            db.session.add(progresso)

        db.session.commit()

        flash(f'Organização "{nome}" adicionada com sucesso!', 'success')
        return redirect(url_for('dashboard'))

    return render_template('adicionar_organizacao.html')

@app.route('/organizacao/<int:organizacao_id>')
def detalhes_organizacao(organizacao_id):
    """Displays the details page for a specific organization, including their migration history."""
    organizacao = Organizacao.query.get_or_404(organizacao_id)
    sessoes = SessaoMigracao.query.filter_by(organizacao_id=organizacao_id).order_by(SessaoMigracao.data_processamento.desc()).all()
    return render_template('detalhes_organizacao.html', organizacao=organizacao, sessoes=sessoes, layouts=LAYOUTS)

@app.route('/download/<int:sessao_id>')
def download_arquivo(sessao_id):
    """Serves the generated TXT file for download."""
    sessao = SessaoMigracao.query.get_or_404(sessao_id)
    if not sessao.arquivo_gerado:
        abort(404)

    diretorio = os.path.join(app.root_path, 'dados')
    return send_from_directory(diretorio, os.path.basename(sessao.arquivo_gerado), as_attachment=True)


# ------------ VALIDATOR FLOW ROUTES ------------ #

@app.route('/organizacao/<int:organizacao_id>/validador/<tipo>', methods=['GET', 'POST'])
def validador(organizacao_id, tipo):
    organizacao = Organizacao.query.get_or_404(organizacao_id)

    if tipo not in LAYOUTS or tipo not in PROCESSORS:
        abort(404)

    processor = PROCESSORS[tipo](LAYOUTS[tipo])
    contexto = LAYOUTS[tipo]

    if request.method == 'POST':
        file = request.files.get('arquivo')
        if not file or not file.filename:
            flash('Arquivo é obrigatório.', 'danger')
            return redirect(url_for('validador', organizacao_id=organizacao_id, tipo=tipo))

        filename = secure_filename(file.filename)
        ext = os.path.splitext(filename)[1].lower()

        try:
            raw_content = file.read()
            df, _, _, alertas = file_processor.detectar_encoding_e_linhas_validas(raw_content, extensao=ext, filename=filename)

            if df is None:
                raise ValueError("Não foi possível processar o arquivo. Verifique o formato e o conteúdo.")

            if alertas:
                flash("\\n".join(alertas), 'warning')

            nova_sessao = SessaoMigracao(
                organizacao_id=organizacao_id, modulo=tipo, arquivo_original=filename,
                total_registros=len(df), status='mapeando'
            )
            db.session.add(nova_sessao)
            db.session.commit()

            mapping_history = mapping_service.load_mapping_history(get_db(), tipo)
            auto_map = processor.auto_map_header(df)
            obrigatorios = [c for c, _, _, _, o in contexto['layout'] if o]

            if not all(auto_map.get(c) for c in obrigatorios):
                auto_map_data = processor.auto_map_by_data(df, mapping_history)
                for campo, col in auto_map_data.items():
                    if campo not in auto_map:
                        auto_map[campo] = col

            session['dataframes'] = {filename: df.to_json(orient='split')}
            session['mapear'] = [{'nome': filename, 'colunas': df.columns.tolist(), 'amostra': df.head(20).where(pd.notnull(df.head(20)), '').to_dict('records'), 'auto_map': auto_map, 'num_registros': len(df)}]
            session['tipo_layout'] = tipo
            session['sessao_id'] = nova_sessao.id
            session['organizacao_id'] = organizacao_id

            # Clear old results from session for the new flow
            session.pop('inconsistencias', None)
            session.pop('stats', None)

        except Exception as e:
            flash(f'Erro ao processar arquivo: {str(e)}', 'danger')
            return redirect(url_for('validador', organizacao_id=organizacao_id, tipo=tipo))

    # GET request logic
    mapear = session.get('mapear', None)
    inconsistencias = session.get('inconsistencias', None)
    stats = session.get('stats', None)

    return render_template(
        "validador.html",
        organizacao=organizacao,
        tipo=tipo,
        nome_rotina=contexto["nome"],
        layout=contexto["layout"],
        js_file=contexto.get("js"),
        mapear=mapear,
        inconsistencias=inconsistencias,
        stats=stats
    )

@app.route('/validador/<tipo>/mapear', methods=['POST'])
def validador_mapear(tipo):
    if tipo not in LAYOUTS or tipo not in PROCESSORS:
        abort(404)

    sessao_id = session.get('sessao_id')
    if not sessao_id:
        flash("Sessão de migração não encontrada. Por favor, inicie o processo novamente.", 'danger')
        return redirect(url_for('dashboard'))

    sessao_migracao = SessaoMigracao.query.get(sessao_id)
    if not sessao_migracao:
        flash("Sessão de migração inválida.", 'danger')
        return redirect(url_for('dashboard'))

    organizacao = sessao_migracao.organizacao
    processor = PROCESSORS[tipo](LAYOUTS[tipo])
    mapear_info = session.get('mapear', [])
    dataframes = session.get('dataframes', {})

    item_data = mapear_info[0]
    nome_arquivo = item_data['nome']
    df = pd.read_json(io.StringIO(dataframes[nome_arquivo]), orient='split')
    df = file_processor.normalizar_colunas_vazias(df)

    mapeamento_usuario = {campo[0]: request.form.get(f"{nome_arquivo}_{campo[0]}") for campo in LAYOUTS[tipo]["layout"]}
    mapeamento_usuario = {k: v for k, v in mapeamento_usuario.items() if v}

    mapping_history = mapping_service.load_mapping_history(get_db(), tipo)
    history_para_salvar = {}
    for campo, col_name in mapeamento_usuario.items():
        if col_name in df.columns:
            serie = df[col_name]
            novas_amostras = processor.aprender_metadados_coluna(serie, campo, mapping_history.get(campo, {}).get("amostras_validas", []))
            if novas_amostras:
                 history_para_salvar[campo] = {"amostras_validas": novas_amostras}

    if history_para_salvar:
        mapping_service.save_mapping_history(get_db(), tipo, history_para_salvar)

    inconsistencias, stats, total_linhas, total_validos, total_invalidos = processor.analisar_dados(df, mapeamento_usuario)

    sessao_migracao.status = 'concluido' if total_invalidos == 0 else 'pendente'
    sessao_migracao.registros_validos = total_validos

    if total_invalidos == 0:
        cnpj_matriz = organizacao.get_cnpj_matriz()
        caminho_arquivo_gerado = processor.exportar_txt(df, mapeamento_usuario, organizacao.codigo, cnpj_matriz)
        sessao_migracao.arquivo_gerado = caminho_arquivo_gerado

    progresso = ProgressoMigracao.query.filter_by(organizacao_id=sessao_migracao.organizacao_id, modulo=tipo).first()
    if progresso:
        percentual = int((total_validos / total_linhas) * 100) if total_linhas > 0 else 0
        progresso.percentual_completo = percentual
        progresso.ultima_atualizacao = datetime.datetime.utcnow()

    db.session.commit()

    session.pop('dataframes', None)
    session.pop('mapear', None)
    session.pop('tipo_layout', None)
    session.pop('sessao_id', None)
    session.pop('total_registros', None)

    flash('Arquivo analisado. Verifique o resultado no histórico da organização.', 'success')
    return redirect(url_for('detalhes_organizacao', organizacao_id=sessao_migracao.organizacao_id))

@app.route('/validador/<tipo>/reset', methods=['POST'])
def validador_reset(tipo):
    session.clear()
    file_processor.limpar_uploads(app.config['UPLOAD_FOLDER'])
    return redirect(url_for('dashboard'))

# ----------- HISTORY ------------------
@app.route('/get_amostra', methods=['POST'])
def get_amostra():
    nome_arquivo = request.form.get('nome_arquivo')
    offset = int(request.form.get('offset', 0))
    limit = int(request.form.get('limit', 20))
    if 'dataframes' not in session or nome_arquivo not in session['dataframes']:
        return jsonify({'ok': False, 'erro': 'Arquivo não encontrado na sessão.'})
    df = pd.read_json(io.StringIO(session['dataframes'][nome_arquivo]), orient='split')
    df = file_processor.normalizar_colunas_vazias(df)
    df_amostra = df.iloc[offset:offset+limit]
    amostra = df_amostra.where(pd.notnull(df_amostra), '').to_dict('records')
    return jsonify({'ok': True, 'amostra': amostra, 'colunas': list(df.columns)})

@app.route('/history_ia', methods=['GET'])
def history_ia():
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