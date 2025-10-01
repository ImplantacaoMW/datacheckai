import os
import secrets

# Configuração do Banco de Dados - Usando SQLite para verificação
DB_HOST = 'localhost'
DB_NAME = 'datacheck_test.db'
DB_USER = 'postgres'
DB_PASS = 'xbala'
DATABASE_URL = f"sqlite:///{DB_NAME}" # Usando SQLite em arquivo para a verificação

# Configuração do Flask
SECRET_KEY = secrets.token_hex(16)
SESSION_TYPE = 'filesystem'
SESSION_FILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'flask_session')
SESSION_PERMANENT = False
UPLOAD_FOLDER = 'uploads'