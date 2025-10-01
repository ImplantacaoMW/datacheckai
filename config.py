import os
import secrets

# Configuração do Banco de Dados
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_NAME = os.environ.get('DB_NAME', 'datacheck')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', 'xbala')
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"


# Configuração do Flask
SECRET_KEY = secrets.token_hex(16)
SESSION_TYPE = 'filesystem'
SESSION_FILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'flask_session')
SESSION_PERMANENT = False
UPLOAD_FOLDER = 'uploads'