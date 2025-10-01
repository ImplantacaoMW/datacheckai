import os
import pandas as pd
from datetime import datetime

class LayoutExporter:
    """Classe base para exportação de arquivos TXT formatados."""

    def __init__(self, modulo, organizacao_codigo, cnpj_matriz, dados_validados):
        """
        Inicializa o exportador.

        Args:
            modulo (str): Nome do módulo (ex: 'mercadorias').
            organizacao_codigo (str): Código da organização.
            cnpj_matriz (str): CNPJ da matriz da organização.
            dados_validados (pd.DataFrame): DataFrame com os dados já validados e formatados.
        """
        self.modulo = modulo
        self.organizacao_codigo = organizacao_codigo
        self.cnpj_matriz = cnpj_matriz
        self.dados = dados_validados
        self.timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        self.filename = f"LAYOUT_{self.modulo.upper()}_{self.organizacao_codigo}_{self.timestamp}.txt"
        self.filepath = os.path.join('dados', self.filename)

    def format_data(self):
        """
        Formata os dados de acordo com as regras específicas do layout.
        Adiciona um cabeçalho com o CNPJ da matriz se ele for fornecido.
        """
        header = ""
        if self.cnpj_matriz:
            header = f"CNPJ:{self.cnpj_matriz}\n"

        # Converte o DataFrame para string, sem o cabeçalho do pandas
        data_string = self.dados.to_csv(index=False, sep='\t', lineterminator='\r\n', header=False)

        return header + data_string

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