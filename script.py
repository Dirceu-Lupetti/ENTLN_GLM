import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta

###############################################################################
#                       FUNÇÕES DE TRATAMENTO ENTLN
###############################################################################

def try_parsing_date(x):
    """
    Tenta converter o valor 'x' em datetime.
    - Se 'x' for numérico, interpreta como timestamp em segundos.
    - Se 'x' for string, tenta formatos c/ e s/ milissegundos.
    - Se falhar tudo, retorna pd.NaT.
    Ao final, arredonda para o segundo mais próximo.
    """
    if isinstance(x, (int, float)):
        dt = pd.to_datetime(x, unit='s')
    else:
        try:
            dt = pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            try:
                dt = pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S')
            except ValueError:
                dt = pd.NaT

    if not pd.isna(dt):
        dt = dt.round('s')
        return dt
    else:
        return pd.NaT

def process_in_batches(df, batch_size=100000):
    """
    Processa a coluna 'datahora' em lotes (batches),
    aplicando a função de parsing de datas (try_parsing_date).
    """
    df = df.reset_index(drop=True)
    n_batches = (len(df) // batch_size) + 1

    for i in range(n_batches):
        start = i * batch_size
        end = min((i + 1) * batch_size, len(df))
        print(f'Processando linhas {start} a {end - 1}')
        df.loc[start:end, 'datahora'] = df.loc[start:end, 'datahora'].apply(try_parsing_date)

    return df

def filtrar_por_datahora(df, start_date, end_date=None, granularity=None):
    """
    Filtra o DataFrame 'df' pela coluna 'datahora', com base em start_date e end_date.
    - granularity='day': filtra por dia inteiro (0h00 até 23h59).
    - granularity='hour': filtra por hora.
    Depois, processa em batches para converter 'datahora' em datetime.
    """
    start_date = pd.to_datetime(start_date)

    if granularity == 'day':
        if end_date is None:
            end_date = start_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        else:
            end_date = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        df['data_somente'] = df['datahora'].astype(str).str.split(' ').str[0]
        df_filtrado = df[(df['data_somente'] >= str(start_date.date())) & 
                         (df['data_somente'] <= str(end_date.date()))]

    elif granularity == 'hour':
        if end_date is None:
            end_date = start_date + pd.Timedelta(hours=1) - pd.Timedelta(seconds=1)
        else:
            end_date = pd.to_datetime(end_date) + pd.Timedelta(seconds=59)
        df['data_hora'] = df['datahora'].astype(str).str.split('.').str[0]
        df_filtrado = df[(df['data_hora'] >= str(start_date)) & 
                         (df['data_hora'] <= str(end_date))]
    else:
        raise ValueError("A granularidade deve ser 'day' ou 'hour'.")

    df_filtrado = process_in_batches(df_filtrado)
    df_filtrado.drop(columns=['data_somente', 'data_hora'], errors='ignore', inplace=True)
    print(f'Quantidade de linhas após o filtro e processamento: {len(df_filtrado)}')

    return df_filtrado

###############################################################################
#                  FUNÇÕES PARA CARREGAR GLM E ENTLN (PANDAS)
###############################################################################

def load_entln_csv(filepath):
    """
    Carrega o CSV do ENTLN (flash) em um DataFrame e padroniza algumas colunas.
    Não faz parse de datahora ainda; isso será feito depois em 'process_in_batches'.
    """
    df_entln = pd.read_csv(filepath)
    df_entln.rename(columns={
        'id': 'entln_id',
        'tipo': 'entln_tipo',
        'datahora': 'datahora',  # Mantemos como 'datahora'
        'latitude': 'entln_lat',
        'longitude': 'entln_lon',
    }, inplace=True)
    return df_entln

def load_glm_csv(filepath):
    """
    Carrega o CSV do GLM em um DataFrame e padroniza colunas.
    """
    df_glm = pd.read_csv(filepath)
    df_glm.rename(columns={
        'flash_lat': 'glm_lat',
        'flash_lon': 'glm_lon',
        'flash_energy': 'glm_energy',
        'product_time': 'glm_time'
    }, inplace=True)
    return df_glm

###############################################################################
#                   FUNÇÕES PARA CORRELACIONAR (FULL JOIN)
###############################################################################

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calcula a distância (km) entre dois pontos (lat1, lon1) e (lat2, lon2)
    usando a fórmula de Haversine.
    """
    R = 6371.0  # Raio médio da Terra em km
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (math.sin(d_lat / 2)**2 
         + math.cos(math.radians(lat1)) 
         * math.cos(math.radians(lat2)) 
         * math.sin(d_lon / 2)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def min_max_scale(series):
    """
    Aplica normalização Min-Max na Series, resultando em valores [0, 1].
    Ignora NaN ao calcular min e max, mas mantém NaN no resultado final.
    """
    s_valid = series.dropna()
    if s_valid.empty:
        return series
    min_val = s_valid.min()
    max_val = s_valid.max()
    if min_val == max_val:
        # Evita divisão por zero se todos os valores forem iguais
        return series.apply(lambda x: 0.5 if pd.notna(x) else np.nan)
    return (series - min_val) / (max_val - min_val)

def add_intensity_scales(df):
    """
    Recebe o DataFrame combinado (GLM+ENTLN) e adiciona
    colunas normalizadas para glm_energy e pico_corrente.
    """
    df = df.copy()
    
    # Normalização Min-Max da energia do GLM
    if 'glm_glm_energy' in df.columns:
        df['glm_energy_scaled'] = min_max_scale(df['glm_glm_energy'])
    else:
        df['glm_energy_scaled'] = np.nan
    
    # Normalização Min-Max do pico de corrente do ENTLN
    if 'entln_pico_corrente' in df.columns:
        # Se quiser usar valor absoluto (ignorar sinal):
        # df['entln_pico_abs'] = df['entln_pico_corrente'].abs()
        # df['entln_pico_scaled'] = min_max_scale(df['entln_pico_abs'])
        
        df['entln_pico_scaled'] = min_max_scale(df['entln_pico_corrente'])
    else:
        df['entln_pico_scaled'] = np.nan

    return df

def parse_glm_time(x):
    """
    Converte a string 'glm_time' em datetime (arredondando se necessário).
    """
    try:
        dt = pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        try:
            dt = pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S')
        except ValueError:
            dt = pd.NaT
    if not pd.isna(dt):
        dt = dt.round('s')
    return dt

def correlate_glm_entln(df_glm, df_entln,
                        time_tolerance_s=30,
                        distance_tolerance_km=10):
    """
    Faz um FULL JOIN entre df_glm e df_entln com tolerância de tempo/distância.
    Retorna DataFrame final.
    """
    # Converter tempo para datetime
    df_glm['glm_dt'] = df_glm['glm_time'].apply(parse_glm_time)
    df_entln['datahora'] = pd.to_datetime(df_entln['datahora'], errors='coerce')

    # Identificadores
    df_glm = df_glm.reset_index(drop=False).rename(columns={'index': 'glm_idx'})
    df_entln = df_entln.reset_index(drop=False).rename(columns={'index': 'entln_idx'})

    matches = []
    time_tol = pd.Timedelta(seconds=time_tolerance_s)

    for i, row_g in df_glm.iterrows():
        g_dt = row_g['glm_dt']
        if pd.isna(g_dt):
            continue

        # Filtrar ENTLN por tempo
        min_t = g_dt - time_tol
        max_t = g_dt + time_tol
        candidate_entln = df_entln[(df_entln['datahora'] >= min_t) &
                                   (df_entln['datahora'] <= max_t)]
        
        g_lat = row_g['glm_lat']
        g_lon = row_g['glm_lon']
        if pd.isna(g_lat) or pd.isna(g_lon):
            # se lat/lon nulos, não correlaciona
            candidate_entln = candidate_entln.iloc[0:0]

        matched_any = False
        for j, row_e in candidate_entln.iterrows():
            e_lat = row_e['entln_lat']
            e_lon = row_e['entln_lon']
            if pd.notna(e_lat) and pd.notna(e_lon):
                dist_km = haversine_distance(g_lat, g_lon, e_lat, e_lon)
                if dist_km <= distance_tolerance_km:
                    matched_any = True
                    combined = {}
                    # Copia colunas GLM (prefixo glm_)
                    for col_g in df_glm.columns:
                        combined[f"glm_{col_g}"] = row_g[col_g]
                    # Copia colunas ENTLN (prefixo entln_)
                    for col_e in df_entln.columns:
                        combined[f"entln_{col_e}"] = row_e[col_e]
                    combined['time_diff_s'] = abs((row_g['glm_dt'] - row_e['datahora']).total_seconds())
                    combined['dist_km'] = dist_km
                    matches.append(combined)

        if not matched_any:
            combined = {}
            for col_g in df_glm.columns:
                combined[f"glm_{col_g}"] = row_g[col_g]
            for col_e in df_entln.columns:
                combined[f"entln_{col_e}"] = np.nan
            combined['time_diff_s'] = np.nan
            combined['dist_km'] = np.nan
            matches.append(combined)

    # ENTLN sem par
    matched_entln_idx = {m['entln_entln_idx'] for m in matches if pd.notna(m['entln_entln_idx'])}
    unmatched_entln = df_entln[~df_entln['entln_idx'].isin(matched_entln_idx)]

    for i, row_e in unmatched_entln.iterrows():
        combined = {}
        for col_g in df_glm.columns:
            combined[f"glm_{col_g}"] = np.nan
        for col_e in df_entln.columns:
            combined[f"entln_{col_e}"] = row_e[col_e]
        combined['time_diff_s'] = np.nan
        combined['dist_km'] = np.nan
        matches.append(combined)

    df_final = pd.DataFrame(matches)
    df_final.sort_values(by=['glm_glm_dt','entln_datahora'], inplace=True, na_position='first')
    df_final.reset_index(drop=True, inplace=True)

    return df_final

###############################################################################
#                   ANÁLISE / COMPARAÇÃO DE VALORES FÍSICOS
###############################################################################

def analyze_physical_correlation(df, use_abs_current=True):
    """
    Exemplo de análise estatística comparando pico_corrente do ENTLN e
    energia do GLM. Converte valores em log10 e calcula correlação.
    - use_abs_current=True indica que vamos usar valor absoluto da corrente.
      Assim, picos negativos e positivos são tratados como 'intensidade' apenas.
    """
    df = df.copy()

    # 1) Extrair colunas
    col_energy = 'glm_glm_energy'
    col_current = 'entln_pico_corrente'

    if col_energy not in df.columns or col_current not in df.columns:
        print("Colunas de energia e/ou pico de corrente não encontradas no DataFrame.")
        return None

    # 2) Se quisermos usar valor absoluto da corrente:
    if use_abs_current:
        df['entln_pico_corrente_abs'] = df[col_current].abs()
        col_current = 'entln_pico_corrente_abs'

    # 3) Evitar log(0) ou log(negativo)
    df['log_energy'] = np.log10(df[col_energy].clip(lower=1e-16))
    df['log_current'] = np.log10(df[col_current].clip(lower=1e-1))

    # 4) Remover NaN
    df_valid = df.dropna(subset=['log_energy','log_current'])

    if len(df_valid) == 0:
        print("Nenhuma linha válida para correlação (tudo NaN ou zero).")
        return None

    # 5) Calcular coeficientes de correlação
    corr_pearson = df_valid[['log_energy','log_current']].corr(method='pearson').iloc[0,1]
    corr_spearman = df_valid[['log_energy','log_current']].corr(method='spearman').iloc[0,1]

    print(f"Correlação Pearson (log(energy) vs log(current)) = {corr_pearson:.4f}")
    print(f"Correlação Spearman (log(energy) vs log(current)) = {corr_spearman:.4f}")

    return (corr_pearson, corr_spearman)

###############################################################################
#                   EXEMPLO DE USO
###############################################################################

def entln_glm():
    """
    Exemplo de pipeline completo:
      1) Carrega e processa ENTLN
      2) Carrega GLM
      3) Faz correlação (FULL JOIN) com tolerâncias
      4) Normaliza intensidades para visualização
      5) Analisa comparações físicas (log scale, correlações)
      6) Salva resultado
    """
    # 1) ENTLN
    path_entln_csv = "ENTLN/flash/data-1656523753578_fev2022_flash.csv"
    df_entln_raw = load_entln_csv(path_entln_csv)
    df_entln_fev15 = filtrar_por_datahora(df_entln_raw,
                                          start_date="2022-02-15",
                                          end_date="2022-02-15",
                                          granularity="day")

    # 2) GLM
    path_glm_csv = "GLM/15022022_petropolis_flashs.csv"
    df_glm_raw = load_glm_csv(path_glm_csv)
    # Se necessário, filtre datas no GLM também...

    # 3) Correlação (full join) com tolerâncias
    df_join = correlate_glm_entln(df_glm_raw, df_entln_fev15,
                                  time_tolerance_s=1,   # ±1s
                                  distance_tolerance_km=10)

    # 4) Normaliza colunas (Min-Max) -> colunas 'glm_energy_scaled' e 'entln_pico_scaled'
    df_join_scaled = add_intensity_scales(df_join)

    # 5) Análise de correlação física (log scale)
    print("\n=== Análise de Correlação Física (GLM vs ENTLN) ===")
    analyze_physical_correlation(df_join_scaled, use_abs_current=True)

    # 6) Salvar CSV final
    df_join_scaled.to_csv("resultado_full_join_GLMENTLN.csv", index=False)
    print("Arquivo 'resultado_full_join_GLMENTLN.csv' gerado com sucesso.")

entln_glm()

