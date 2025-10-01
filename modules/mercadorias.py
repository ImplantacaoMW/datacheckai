import re
import pandas as pd
from services import mapping_service, validation_service, export_service

class MercadoriasProcessor:
    """Processador para o layout de Mercadorias."""

    def __init__(self, layout_config):
        self.layout_config = layout_config
        self.layout = layout_config['layout']
        self.keywords = layout_config['keywords']
        self.campos_ignorar_ia = [
            'preco_venda', 'preco_custo_aquisicao', 'preco_venda_sugerido', 'preco_garantia',
            'preco_custo_fabrica', 'origem', 'qtd_embalagem'
        ]

    def validar_campo(self, campo, valor):
        """Valida um único campo de acordo com as regras do layout de mercadorias."""
        if isinstance(valor, str):
            valor_limpo = valor.strip()
        else:
            valor_limpo = valor

        layout_info = next((item for item in self.layout if item[0] == campo), None)
        if not layout_info:
            return True

        _, _, tipo, tamanho, obrigatorio = layout_info

        if obrigatorio and validation_service.is_vazio(valor_limpo):
            return False
        if not obrigatorio and validation_service.is_vazio(valor_limpo):
            return True

        max_len = tamanho if isinstance(tamanho, int) else float('inf')

        if campo == 'codigo':
            try:
                s = str(valor_limpo)
                if not (4 <= len(s) <= max_len and re.match(r'^[A-Z0-9\s\-\/\.]+$', s, re.I)):
                    return False
                common_words = ['de', 'da', 'do', 'com', 'para', 'em', 'um', 'uma']
                if any(word in s.lower().split() for word in common_words):
                    return False
                return True
            except Exception:
                return False
        if campo == 'nome':
            return isinstance(valor_limpo, str) and 5 <= len(str(valor_limpo)) <= 150
        if campo in ['unidade', 'marca', 'tipo', 'tributacao', 'aplicacao', 'coeficiente', 'cod_original']:
            return isinstance(valor_limpo, str) and len(str(valor_limpo)) <= max_len
        if campo == 'ncm':
            if isinstance(valor_limpo, (int, float)):
                valor_limpo = str(int(valor_limpo))
            return (isinstance(valor_limpo, str) and
                    (len(valor_limpo) == 8 or (len(valor_limpo) == 10 and '.' in valor_limpo)) and
                    re.match(r'^[0-9\.]+$', valor_limpo))
        if campo in ['preco_venda', 'preco_custo_aquisicao', 'preco_venda_sugerido', 'preco_garantia', 'preco_custo_fabrica']:
            try:
                num = float(str(valor_limpo).replace(',', '.'))
                return num >= 0
            except (ValueError, TypeError):
                return False
        if campo == 'original':
            return str(valor_limpo).strip().lower() in ['1', '0', 'true', 'false', 'sim', 'não', 'nao', 'yes', 'no']
        if campo in ['curva_abc', 'curva_xyz']:
            return str(valor_limpo).strip().upper() in ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']
        if campo in ['origem', 'anp', 'qtd_embalagem']:
            if campo == 'anp':
                v = str(valor_limpo).strip()
                if v == "": return True
                if v.isdigit() and len(v) == 9: return True
                try:
                    v_int = int(float(v))
                    return len(str(v_int)) == 9
                except (ValueError, TypeError):
                    return False
            else:
                try:
                    return str(valor_limpo).strip() == "" or float(str(valor_limpo).replace(',', '.')) >= 0
                except (ValueError, TypeError):
                    return False
        if campo == 'cest':
            if isinstance(valor_limpo, (int, float)):
                valor_limpo = str(int(valor_limpo))
            return (isinstance(valor_limpo, str) and
                    (len(valor_limpo) == 7 or (len(valor_limpo) == 9 and '.' in valor_limpo)) and
                    re.match(r'^[0-9\.]+$', valor_limpo))
        return True

    def analisar_dados(self, df, mapeamento):
        """Realiza a análise completa do DataFrame, identificando inconsistências."""
        inconsistencias = {}
        stats = []
        total_linhas = len(df)
        linha_valida = [True] * total_linhas

        # --- Validação de Códigos ---
        codigos_duplicados = set()
        codigos_vistos = set()

        codigo_col = mapeamento.get('codigo')
        if codigo_col and codigo_col in df.columns:
            for idx, v in enumerate(df[codigo_col]):
                if not self.validar_campo('codigo', v):
                    linha_valida[idx] = False

                if not validation_service.is_vazio(v):
                    valor_str = str(v).strip()
                    if valor_str in codigos_vistos:
                        codigos_duplicados.add(valor_str)
                    else:
                        codigos_vistos.add(valor_str)

        if codigos_duplicados:
            # Marca todas as ocorrências de códigos duplicados como inválidas
            for idx, v in enumerate(df[codigo_col]):
                if str(v).strip() in codigos_duplicados:
                    linha_valida[idx] = False
            inconsistencias['codigo_duplicado'] = {"label": "Código", "tipo": "duplicado", "mensagem": f"Códigos duplicados: {len(codigos_duplicados)} códigos.", "amostra": list(codigos_duplicados)[:8]}

        # --- Validação de outros campos ---
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
        Gera o arquivo TXT formatado para o layout de mercadorias, garantindo
        a ordem correta das colunas.
        """
        # Define a ordem correta das colunas com base no layout
        colunas_layout = [campo[0] for campo in self.layout]

        df_export = pd.DataFrame()

        # Preenche o novo DataFrame com os dados das colunas mapeadas, na ordem correta
        for campo_layout in colunas_layout:
            col_mapeada = mapeamento.get(campo_layout)
            if col_mapeada and col_mapeada in df_original.columns:
                df_export[campo_layout] = df_original[col_mapeada]
            else:
                # Adiciona uma coluna vazia se o campo do layout não foi mapeado
                df_export[campo_layout] = ""

        # Instancia o exportador com o DataFrame formatado
        exporter = export_service.LayoutExporter(
            modulo='mercadorias',
            organizacao_codigo=organizacao_codigo,
            cnpj_matriz=cnpj_matriz,
            dados_validados=df_export
        )

        return exporter.export()