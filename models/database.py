from flask_sqlalchemy import SQLAlchemy
import datetime

db = SQLAlchemy()

class Cliente(db.Model):
    __tablename__ = 'clientes'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.String(20), unique=True, nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    cnpj = db.Column(db.String(14))
    data_cadastro = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    ativo = db.Column(db.Boolean, default=True)
    sessoes = db.relationship("SessaoMigracao", back_populates="cliente", cascade="all, delete-orphan")
    progressos = db.relationship("ProgressoMigracao", back_populates="cliente", cascade="all, delete-orphan")

class SessaoMigracao(db.Model):
    __tablename__ = 'sessoes_migracao'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey('clientes.id'), nullable=False)
    modulo = db.Column(db.String(50), nullable=False)
    arquivo_original = db.Column(db.String(255))
    arquivo_gerado = db.Column(db.String(255))
    total_registros = db.Column(db.Integer)
    registros_validos = db.Column(db.Integer)
    data_processamento = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    status = db.Column(db.String(20), default='processando')
    cliente = db.relationship("Cliente", back_populates="sessoes")

class ProgressoMigracao(db.Model):
    __tablename__ = 'progresso_migracao'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey('clientes.id'), nullable=False)
    modulo = db.Column(db.String(50), nullable=False)
    percentual_completo = db.Column(db.Integer, default=0)
    ultima_atualizacao = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    cliente = db.relationship("Cliente", back_populates="progressos")

def init_db(app):
    """Inicializa o banco de dados e cria as tabelas se não existirem."""
    db.init_app(app)
    with app.app_context():
        db.create_all()