from flask_sqlalchemy import SQLAlchemy
import datetime

db = SQLAlchemy()

class Organizacao(db.Model):
    __tablename__ = 'organizacoes'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.String(20), unique=True, nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    data_cadastro = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    ativo = db.Column(db.Boolean, default=True)

    # Relacionamentos
    cnpjs = db.relationship("CNPJ", back_populates="organizacao", cascade="all, delete-orphan")
    sessoes = db.relationship("SessaoMigracao", back_populates="organizacao", cascade="all, delete-orphan")
    progressos = db.relationship("ProgressoMigracao", back_populates="organizacao", cascade="all, delete-orphan")

    def get_cnpj_matriz(self):
        """Retorna o CNPJ da matriz."""
        for cnpj in self.cnpjs:
            if cnpj.is_matriz:
                return cnpj.numero
        return None

class CNPJ(db.Model):
    __tablename__ = 'cnpjs'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    numero = db.Column(db.String(14), unique=True, nullable=False)
    is_matriz = db.Column(db.Boolean, default=False, nullable=False)
    organizacao_id = db.Column(db.Integer, db.ForeignKey('organizacoes.id'), nullable=False)
    organizacao = db.relationship("Organizacao", back_populates="cnpjs")

class SessaoMigracao(db.Model):
    __tablename__ = 'sessoes_migracao'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    organizacao_id = db.Column(db.Integer, db.ForeignKey('organizacoes.id'), nullable=False)
    modulo = db.Column(db.String(50), nullable=False)
    arquivo_original = db.Column(db.String(255))
    arquivo_gerado = db.Column(db.String(255))
    total_registros = db.Column(db.Integer)
    registros_validos = db.Column(db.Integer)
    data_processamento = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    status = db.Column(db.String(20), default='processando')
    organizacao = db.relationship("Organizacao", back_populates="sessoes")

class ProgressoMigracao(db.Model):
    __tablename__ = 'progresso_migracao'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    organizacao_id = db.Column(db.Integer, db.ForeignKey('organizacoes.id'), nullable=False)
    modulo = db.Column(db.String(50), nullable=False)
    percentual_completo = db.Column(db.Integer, default=0)
    ultima_atualizacao = db.Column(db.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    organizacao = db.relationship("Organizacao", back_populates="progressos")

def init_db(app):
    """Inicializa o banco de dados e cria as tabelas se não existirem."""
    db.init_app(app)
    with app.app_context():
        db.create_all()