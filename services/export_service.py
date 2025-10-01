import os
import pandas as pd
from datetime import datetime

class LayoutExporter:
    """Classe base para exportação de arquivos TXT formatados."""

    def __init__(self, modulo, cliente_id, dados_validados):
        """
        Inicializa o exportador.

        Args:
            modulo (str): Nome do módulo (ex: 'mercadorias').
            cliente_id (str): Identificador do cliente.
            dados_validados (pd.DataFrame): DataFrame com os dados já validados e normalizados.
        """
        self.modulo = modulo
        self.cliente_id = cliente_id
        self.dados = dados_validados
        self.timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        self.filename = f"LAYOUT_{self.modulo.upper()}_{self.cliente_id}_{self.timestamp}.txt"
        self.filepath = os.path.join('dados', self.filename)  # Salvar na pasta 'dados'

    def format_data(self):
        """
        Formata os dados de acordo com as regras específicas do layout.
        Este método deve ser sobrescrito pelas classes filhas.
        """
        # Exemplo de implementação padrão: converter para CSV com TAB
        # As classes específicas irão formatar campos, tamanhos, etc.
        return self.dados.to_csv(index=False, sep='\t', lineterminator='\r\n', header=False)

    def export(self):
        """
        Gera e salva o arquivo TXT formatado.
        """
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

        formatted_string = self.format_data()

        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                f.write(formatted_string)
            print(f"Arquivo gerado com sucesso em: {self.filepath}")
            return self.filepath
        except Exception as e:
            print(f"Erro ao exportar arquivo {self.filename}: {e}")
            return None