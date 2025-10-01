import unicodedata
import math
import pandas as pd
from decimal import Decimal

def is_vazio(valor):
    """Verifica se um valor é considerado vazio (None, NaN, string vazia)."""
    if valor is None:
        return True
    if isinstance(valor, float) and math.isnan(valor):
        return True
    if isinstance(valor, str) and (valor.strip() == "" or valor.strip().lower() == "nan"):
        return True
    try:
        if pd.isna(valor):
            return True
    except Exception:
        pass
    return False

def remover_acentos(texto):
    """Remove acentos de uma string."""
    if not isinstance(texto, str):
        texto = str(texto)
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

def normalizar(texto):
    """Normaliza uma string: remove acentos e converte para minúsculas."""
    return remover_acentos(texto).strip().lower()

def convert_decimals(obj):
    """Converte recursivamente objetos Decimal para string em listas e dicionários."""
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return str(obj)
    return obj