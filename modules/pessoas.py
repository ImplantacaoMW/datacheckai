import re
import pandas as pd
import datetime
from services import mapping_service, validation_service, export_service

class PessoasProcessor:
    """Processador para o layout de Pessoas."""

    def __init__(self, layout_config):
        self.layout_config = layout_config
        self.layout = layout_config['layout']
        self.keywords = layout_config['keywords']
        self.campos_ignorar_ia = [] # Nenhum campo a ignorar por padrão

    def normalizar_tipos(self, df, mapeamento):
        """
        Normaliza colunas de opção (ex: PF/PJ, estado civil) para os valores padrão do layout.
        """
        # --- MAPS PARA TODOS OS CAMPOS DE OPÇÃO ---
        mapas = {
            "tipo_pessoa": {'pf': '1', 'f': '1', 'fisica': '1', 'física': '1', '1': '1', 'pj': '2', 'j': '2', 'juridica': '2', 'jurídica': '2', '2': '2'},
            "tipo_contribuinte": {'icms': '1', 'contribuinte': '1', '1': '1', 'isento': '2', 'não contribuinte': '2', 'nao contribuinte': '2', '2': '2', '9': '9'},
            "sexo": {'f': '1', 'feminino': '1', '1': '1', 'm': '2', 'masculino': '2', '2': '2'},
            "estado_civil": {'casado': '1', 'casado(a)': '1', '1': '1', 'solteiro': '2', 'solteiro(a)': '2', '2': '2', 'separado': '3', 'separado(a)': '3', '3': '3', 'viuvo': '4', 'viúvo': '4', 'viuvo(a)': '4', 'viúvo(a)': '4', '4': '4', 'desquitado': '5', 'desquitado(a)': '5', '5': '5', 'divorciado': '6', 'divorciado(a)': '6', '6': '6', 'outros': '7', 'outro': '7', 'outra': '7', '7': '7'},
            "tipo_endereco": {'residencial': '1', '1': '1', 'comercial': '2', '2': '2', 'cobranca': '3', 'cobrança': '3', '3': '3', 'secundario': '4', 'secundário': '4', '4': '4', 'entrega': '5', '5': '5', 'coleta': '6', '6': '6'},
            "tipo_telefone": {'celular': '1', 'cel': '1', 'celular comercial': '1', '1': '1', 'fixo': '2', 'residencial': '2', 'telefone fixo': '2', 'comercial': '2', '2': '2', 'fax comercial': '3', '3': '3', 'fax residencial': '4', '4': '4', 'nextel': '5', '5': '5'},
            "produtor_rural": {'1': '1', 'true': '1', 'sim': '1', 'yes': '1', '0': '0', 'false': '0', 'nao': '0', 'não': '0', 'no': '0'}
        }

        df_normalizado = df.copy()
        for campo, mapa in mapas.items():
            col_mapeada = mapeamento.get(campo)
            if col_mapeada and col_mapeada in df_normalizado.columns:
                df_normalizado[col_mapeada] = df_normalizado[col_mapeada].astype(str).apply(validation_service.normalizar).replace(mapa)

        return df_normalizado

    def validar_campo(self, campo, valor):
        """Valida um único campo de acordo com as regras do layout de pessoas."""
        valor_limpo = str(valor).strip() if valor is not None else ""

        layout_info = next((item for item in self.layout if item[0] == campo), None)
        if not layout_info:
            return True

        _, _, tipo, tamanho, obrigatorio = layout_info

        if obrigatorio and validation_service.is_vazio(valor_limpo):
            return False
        if not obrigatorio and validation_service.is_vazio(valor_limpo):
            return True

        # Validações específicas
        if campo == "cpf_cnpj":
            s = re.sub(r'[^0-9]', '', valor_limpo)
            return len(s) in [11, 14]

        if campo in ["tipo_pessoa", "tipo_contribuinte", "sexo", "estado_civil", "tipo_endereco", "tipo_telefone", "produtor_rural"]:
            return valor_limpo in [str(i) for i in range(1, 10)]

        if tipo.lower() == 'numérico':
            try:
                float(str(valor_limpo).replace(',', '.'))
                return True
            except (ValueError, TypeError):
                return False

        if tipo.lower() == 'booleano':
            return validation_service.normalizar(valor_limpo) in ['1', '0', 'true', 'false', 'sim', 'nao', 'não', 'yes', 'no']

        if tipo.lower() == 'data':
            for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y'):
                try:
                    datetime.datetime.strptime(valor_limpo, fmt)
                    return True
                except (ValueError, TypeError):
                    continue
            return False

        if tipo.lower() == 'texto':
            return not (tamanho and isinstance(tamanho, int) and len(valor_limpo) > tamanho)

        return True

    def analisar_dados(self, df, mapeamento):
        """Realiza a análise completa do DataFrame, identificando inconsistências."""
        inconsistencias = {}
        stats = []
        total_linhas = len(df)
        linha_valida = [True] * total_linhas

        # Normaliza os dados primeiro, pois a validação depende dos tipos normalizados
        df_normalizado = self.normalizar_tipos(df, mapeamento)

        # --- Validação de CPF/CNPJ (Duplicidade) ---
        cpf_cnpj_col = mapeamento.get('cpf_cnpj')
        if cpf_cnpj_col and cpf_cnpj_col in df_normalizado.columns:
            # Remove caracteres não numéricos para a verificação de duplicidade
            cpf_cnpj_limpos = df_normalizado[cpf_cnpj_col].astype(str).str.replace(r'[^0-9]', '', regex=True)
            duplicados = cpf_cnpj_limpos[cpf_cnpj_limpos.duplicated(keep=False)]

            if not duplicados.empty:
                indices_duplicados = duplicados.index
                for idx in indices_duplicados:
                    linha_valida[idx] = False

                valores_duplicados = df_normalizado[cpf_cnpj_col].loc[indices_duplicados].unique()
                inconsistencias['cpf_cnpj_duplicado'] = {
                    "label": "CPF / CNPJ", "tipo": "duplicado",
                    "mensagem": f"Documentos duplicados: {len(valores_duplicados)} valor(es).",
                    "amostra": list(valores_duplicados)[:8]
                }

        # --- Validação campo a campo ---
        for campo, label, tipo, tamanho, obrigatorio in self.layout:
            col = mapeamento.get(campo)

            if not col or col not in df_normalizado.columns:
                if obrigatorio:
                    inconsistencias[f'{campo}_nao_mapeado'] = {"label": label, "tipo": "invalido", "mensagem": "Campo obrigatório não mapeado.", "amostra": []}
                    for i in range(total_linhas): linha_valida[i] = False
                stats.append({'campo': label, 'validos': 0, 'invalidos': total_linhas if obrigatorio else 0})
                continue

            serie = df_normalizado[col]
            validos, invalidos = 0, 0
            amostra_invalidos = set()

            for idx, valor in serie.items():
                if self.validar_campo(campo, valor):
                    validos += 1
                else:
                    invalidos += 1
                    if obrigatorio:
                        linha_valida[idx] = False
                    if not validation_service.is_vazio(valor):
                        amostra_invalidos.add(str(valor))

            stats.append({'campo': label, 'validos': validos, 'invalidos': invalidos})

            if amostra_invalidos:
                inconsistencias[campo] = {"label": label, "tipo": "invalido", "mensagem": f"Possui {invalidos} valor(es) inválido(s)", "amostra": sorted(list(amostra_invalidos))[:8]}

        total_validos_geral = sum(linha_valida)
        total_invalidos_geral = total_linhas - total_validos_geral

        return dict(sorted(inconsistencias.items(), key=lambda x: x[1]['label'])), stats, total_linhas, total_validos_geral, total_invalidos_geral

    def auto_map_header(self, df):
        """Mapeia cabeçalhos baseado na similaridade de nomes."""
        auto_map = {}
        for campo, *_ in self.layout:
            melhor_col, melhor_score = None, 0
            for col in df.columns:
                col_norm = mapping_service.normalizar_nome(col)
                for key in self.keywords.get(campo, [campo]):
                    key_norm = mapping_service.normalizar_nome(key)
                    score = mapping_service.similaridade(col_norm, key_norm)
                    if score > melhor_score and score >= 0.82:
                        melhor_col, melhor_score = col, score
            if melhor_col:
                auto_map[campo] = melhor_col
        return auto_map

    def auto_map_by_data(self, df, mapping_history):
        """Mapeia colunas baseado no histórico de dados (IA)."""
        auto_map = {}
        for campo, *_ in self.layout:
            if campo in mapping_history and mapping_history[campo].get("amostras_validas"):
                campo_amostras = set(mapping_history[campo]["amostras_validas"])
                melhor_col, melhor_score = None, 0
                for col in df.columns:
                    serie = df[col].dropna().astype(str)
                    serie_validas = set([v for v in serie if self.validar_campo(campo, v)])
                    if not serie_validas:
                        continue
                    intersecao = campo_amostras.intersection(serie_validas)
                    score = len(intersecao) / max(len(serie_validas), 1)
                    if score > melhor_score and score >= 0.5:
                        melhor_col, melhor_score = col, score
                if melhor_col:
                    auto_map[campo] = melhor_col
        return auto_map

    def aprender_metadados_coluna(self, serie, campo_layout, old_samples=None):
        """Aprende novas amostras válidas de uma coluna."""
        if campo_layout in self.campos_ignorar_ia:
            return old_samples or []
        valid_samples = set(s for s in (old_samples or []) if s != "")
        for val in pd.Series(serie.dropna().astype(str).unique()):
            if self.validar_campo(campo_layout, val) and str(val).strip() != "":
                valid_samples.add(str(val))
        return list(valid_samples)

    def exportar_txt(self, df_original, mapeamento, organizacao_codigo, cnpj_matriz):
        """
        Normaliza, formata e gera o arquivo TXT para o layout de pessoas,
        garantindo a ordem correta das colunas.
        """
        # 1. Normaliza os dados de opção (PF/PJ, etc.)
        df_normalizado = self.normalizar_tipos(df_original, mapeamento)

        # 2. Garante a ordem e o formato corretos para exportação
        colunas_layout = [campo[0] for campo in self.layout]
        df_export = pd.DataFrame()

        for campo_layout in colunas_layout:
            col_mapeada = mapeamento.get(campo_layout)
            if col_mapeada and col_mapeada in df_normalizado.columns:
                df_export[campo_layout] = df_normalizado[col_mapeada]
            else:
                df_export[campo_layout] = ""

        # 3. Instancia o exportador com o DataFrame final
        exporter = export_service.LayoutExporter(
            modulo='pessoas',
            organizacao_codigo=organizacao_codigo,
            cnpj_matriz=cnpj_matriz,
            dados_validados=df_export
        )
        return exporter.export()