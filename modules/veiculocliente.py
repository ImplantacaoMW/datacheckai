import re
import pandas as pd
import datetime
from services import mapping_service, validation_service, export_service

class VeiculoClienteProcessor:
    """Processador para o layout de Veículos do Cliente."""

    def __init__(self, layout_config):
        self.layout_config = layout_config
        self.layout = layout_config['layout']
        self.keywords = layout_config['keywords']
        self.campos_ignorar_ia = [] # Nenhum campo a ignorar por padrão

    def validar_campo(self, campo, valor):
        """Valida um único campo de acordo com as regras do layout de veículos."""
        valor_limpo = str(valor).strip() if valor is not None else ""

        layout_info = next((item for item in self.layout if item[0] == campo), None)
        if not layout_info:
            return True

        _, _, tipo, tamanho, obrigatorio = layout_info

        if obrigatorio and validation_service.is_vazio(valor_limpo):
            return False
        if not obrigatorio and validation_service.is_vazio(valor_limpo):
            return True

        max_len = tamanho if isinstance(tamanho, int) else float('inf')

        if campo == 'cpf_cnpj':
            s = re.sub(r'[^0-9]', '', valor_limpo)
            return len(s) in [11, 14]

        if campo == 'placa':
            s = valor_limpo.upper().replace("-", "")
            return len(s) == 7

        if campo in ['ano_fabricacao', 'ano_modelo']:
            return valor_limpo.isdigit() and len(valor_limpo) == 4 and 1900 <= int(valor_limpo) <= 2100

        if campo == 'chassi':
            return 1 <= len(valor_limpo) <= max_len

        if tipo.lower() == 'data':
            for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y'):
                try:
                    datetime.datetime.strptime(valor_limpo, fmt)
                    return True
                except (ValueError, TypeError):
                    continue
            return False

        if tipo.lower() == 'timestamp':
            for fmt in ('%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'):
                try:
                    datetime.datetime.strptime(valor_limpo, fmt)
                    return True
                except (ValueError, TypeError):
                    continue
            return False

        if tipo.lower() == 'texto':
            return len(valor_limpo) <= max_len

        return True

    def analisar_dados(self, df, mapeamento):
        """Realiza a análise completa do DataFrame, identificando inconsistências."""
        inconsistencias = {}
        stats = []
        total_linhas = len(df)
        linha_valida = [True] * total_linhas

        # --- Validação de Duplicidade (Chassi e Placa) ---
        for campo_chave in ['chassi', 'placa']:
            col_mapeada = mapeamento.get(campo_chave)
            if col_mapeada and col_mapeada in df.columns:
                # Ignora valores vazios na verificação de duplicidade
                series_sem_vazios = df[col_mapeada][~df[col_mapeada].apply(validation_service.is_vazio)]
                duplicados = series_sem_vazios[series_sem_vazios.duplicated(keep=False)]

                if not duplicados.empty:
                    indices_duplicados = duplicados.index
                    for idx in indices_duplicados:
                        linha_valida[idx] = False

                    label_campo = next((item[1] for item in self.layout if item[0] == campo_chave), campo_chave)
                    valores_duplicados = duplicados.unique()
                    inconsistencias[f'{campo_chave}_duplicado'] = {
                        "label": label_campo, "tipo": "duplicado",
                        "mensagem": f"Valores duplicados: {len(valores_duplicados)}.",
                        "amostra": list(valores_duplicados)[:8]
                    }

        # --- Validação campo a campo ---
        for campo, label, tipo, tamanho, obrigatorio in self.layout:
            col = mapeamento.get(campo)

            if not col or col not in df.columns:
                if obrigatorio:
                    inconsistencias[f'{campo}_nao_mapeado'] = {"label": label, "tipo": "invalido", "mensagem": "Campo obrigatório não mapeado.", "amostra": []}
                    for i in range(total_linhas): linha_valida[i] = False
                stats.append({'campo': label, 'validos': 0, 'invalidos': total_linhas if obrigatorio else 0})
                continue

            serie = df[col]
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
        Gera o arquivo TXT formatado para o layout de veículos, garantindo
        a ordem correta das colunas.
        """
        colunas_layout = [campo[0] for campo in self.layout]
        df_export = pd.DataFrame()

        for campo_layout in colunas_layout:
            col_mapeada = mapeamento.get(campo_layout)
            if col_mapeada and col_mapeada in df_original.columns:
                df_export[campo_layout] = df_original[col_mapeada]
            else:
                df_export[campo_layout] = ""

        exporter = export_service.LayoutExporter(
            modulo='veiculos_cliente',
            organizacao_codigo=organizacao_codigo,
            cnpj_matriz=cnpj_matriz,
            dados_validados=df_export
        )
        return exporter.export()