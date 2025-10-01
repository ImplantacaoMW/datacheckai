import os
import io
import csv
import re
import pandas as pd
from werkzeug.utils import secure_filename

def limpar_uploads(upload_folder):
    """Remove todos os arquivos da pasta de uploads."""
    for f in os.listdir(upload_folder):
        caminho_f = os.path.join(upload_folder, f)
        try:
            if os.path.isfile(caminho_f):
                os.remove(caminho_f)
        except Exception as e:
            print(f"Erro ao limpar arquivo {caminho_f}: {e}")

def normalizar_colunas_vazias(df):
    """Renomeia colunas sem nome (ex: 'Unnamed: 1') para um formato legível."""
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

def detectar_encoding_e_linhas_validas(file_bytes, extensao='.csv', filename='arquivo'):
    """
    Detecta o encoding, o separador e as linhas válidas de um arquivo CSV ou Excel.
    Retorna um DataFrame do Pandas, o separador, o encoding e uma lista de alertas.
    """
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
            sep_heur = max(sep_counts, key=sep_counts.get) if any(sep_counts.values()) else ','

        seps_to_try = [sep_heur] + [s for s in delimiters if s != sep_heur]

        for sep in seps_to_try:
            try:
                # Lógica para detectar e alertar sobre quebras de linha dentro de campos
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

                # Lógica para ignorar linhas com número de colunas inconsistente
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
                    elif n_cols is not None and len(row) != n_cols:
                        linhas_ignoradas.add(idx_linha_arq)
                    else:
                        rows.append(row)

                # Reconstrói o CSV apenas com linhas válidas
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
                            f'Atenção: O arquivo "{filename}" contém {len(linhas_ignoradas)} linha(s) com número de colunas diferente do cabeçalho. '
                            f'Essas linhas foram ignoradas. Linhas: {exemplo}.'
                        )
                    linhas_ignoradas_indices = alertas
            except Exception:
                continue
        if melhor_df is not None:
            break

    return melhor_df, melhor_sep, melhor_enc, linhas_ignoradas_indices