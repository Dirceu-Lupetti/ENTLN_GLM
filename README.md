README

Este README documenta o processo de carregamento, tratamento, correlação e análise de dados de raios provenientes de duas fontes distintas:
	1.	ENTLN (Earth Networks Total Lightning Network) – Rede terrestre que mede descargas atmosféricas em solo, fornecendo informações como pico de corrente, coordenadas e timestamp.
	2.	GLM (Geostationary Lightning Mapper) – Sensor a bordo dos satélites GOES-16/GOES-17, que detecta energia óptica emitida pelos raios no topo das nuvens, também com registros de localização e timestamp.

Abaixo estão:
	•	Introdução às bases de dados e sensores
	•	Grandezas físicas envolvidas
	•	Método de comparação e junção temporal e espacial (Full Join)
	•	Tratamento utilizado (limpeza, normalização, batch processing de datas)
	•	Implicações da correlação e associação dessas duas fontes

1. Introdução às Bases de Dados

ENTLN – Earth Networks Total Lightning Network
	•	Tipo: Rede terrestre de sensores que “escuta” as descargas atmosféricas (raios).
	•	Cobertura: Mundial, porém com maior densidade de sensores em certas regiões.
	•	Principais colunas:
	•	entln_lat, entln_lon: latitude e longitude do raio detectado.
	•	datahora: timestamp do evento (em UTC), podendo estar em diferentes formatos (string, float/timestamp em segundos, etc.).
	•	pico_corrente: intensidade (em kA ou A) do pulso elétrico (pode ser positiva ou negativa).
	•	Particularidades:
	•	Pode detectar tanto raios nuvem-solo (CG) quanto intra-nuvem (IC).
	•	O pico de corrente fornece uma medida elétrica do raio.

GLM – Geostationary Lightning Mapper (GOES-16/17)
	•	Tipo: Instrumento óptico a bordo de satélites geoestacionários operados pela NOAA, com cobertura sobre o Hemisfério Ocidental.
	•	Principais colunas:
	•	glm_lat, glm_lon: localização do flash (detectado no topo das nuvens).
	•	glm_energy: energia ótica total estimada (Joules ou Joules-equivalente).
	•	glm_time: momento do evento (UTC).
	•	Particularidades:
	•	Detecta emissões luminosas dentro da nuvem.
	•	A energia vista pelo GLM depende de fatores como profundidade da nuvem, intensidade do pulso, espessura, hora do dia (iluminação solar) etc.

2. Grandezas Físicas

ENTLN – Pico de Corrente (kA)
	•	Mede a corrente elétrica do raio:
	•	Valores negativos ou positivos (indicam polaridade).
	•	Maior corrente nem sempre implica maior emissão óptica (depende da geometria do raio, altitude, etc.).

GLM – Energia Óptica (J)
	•	Quantidade de luz detectada pelo sensor no topo da nuvem.
	•	Altamente sensível à atenuação na nuvem, geometria de observação e intensidade real do raio.

Importante: Não há uma fórmula direta que converta “kA” para “J” e vice-versa. São indicadores diferentes do mesmo fenômeno (raio), cada qual influenciado por condições diversas.

3. Método de Comparação e Junção (Full Join)

Como as duas bases medem descargas atmosféricas com timestamps e coordenadas, podemos tentar associar um evento ENTLN a um evento GLM. Para isso:
	1.	Tolerância Temporal: Verificamos se as detecções ocorreram em um intervalo de tempo próximo (por exemplo, ±30 segundos).
	2.	Tolerância Espacial: Verificamos a distância entre (lat, lon) do evento ENTLN e do evento GLM, usando a fórmula de Haversine (limite, por exemplo, 10 km).
	3.	Se um evento GLM estiver dentro dos limites de tempo e espaço de um evento ENTLN, consideramos que ambos correspondem ao mesmo “raio” ou “flash”.
	4.	Full Join:
	•	Incluímos também eventos que não encontraram par em outra base (GLM sozinho ou ENTLN sozinho), mas que ainda são relevantes para análise (não houve detecção cruzada).

4. Tratamento Utilizado

4.1. Limpeza e Padronização de Datas (ENTLN)

A coluna datahora pode vir em diferentes formatos (strings, timestamps numéricos, milissegundos, etc.). Para uniformizar:
	•	try_parsing_date(): Tenta converter cada valor para datetime (UTC) usando formatos sucessivos (%Y-%m-%d %H:%M:%S.%f, %Y-%m-%d %H:%M:%S, ou timestamp em segundos).
	•	Arredonda para o segundo mais próximo (facilita comparações).
	•	process_in_batches(): Caso o arquivo seja muito grande, processa a coluna em lotes (batches) para evitar sobrecarregar a memória.

4.2. Filtro Opcional por Intervalo de Datas
	•	filtrar_por_datahora(): Permite selecionar, por exemplo, apenas um dia específico (granularidade de ‘day’) ou uma hora (granularity='hour'), reduzindo o volume de dados e focando num período de interesse (por exemplo, 15/02/2022).

4.3. Padronização das Colunas
	•	ENTLN → entln_lat, entln_lon, entln_tipo, entln_pico_corrente, etc.
	•	GLM → glm_lat, glm_lon, glm_energy, glm_time, etc.

4.4. Cálculo de Distância via Haversine

A função haversine_distance(lat1, lon1, lat2, lon2) calcula a distância entre dois pontos (em graus decimais) na superfície terrestre (aproximação esférica). Isso define se dois raios estão próximos o suficiente (ex.: ≤ 10 km) para serem considerados o mesmo evento.

4.5. Associação Final
	•	correlate_glm_entln():
	1.	Converte glm_time e datahora para datetime.
	2.	Para cada evento GLM, busca eventos ENTLN no range de tempo ±time_tolerance_s.
	3.	Verifica a distância com Haversine. Se for ≤ distance_tolerance_km, gera uma linha casada no DataFrame final.
	4.	Se não encontrar par (GLM sozinho), cria linha “GLM only”.
	5.	Se algum ENTLN não foi usado (porque não bateu em nenhum GLM), cria linha “ENTLN only”.
	6.	Resulta em um Full Join.

4.6. Normalização para Visualização (Min-Max)
	•	add_intensity_scales() cria colunas _scaled para comparar glm_energy e pico_corrente em [0, 1], útil em mapas e gráficos.
	•	Lembrando que não é uma unificação física, apenas uma escala comparativa.

5. Implicações e Análise

5.1. Natureza Diferente das Grandezas
	•	O pico de corrente (kA) está ligado diretamente à descarga elétrica (rede terrestre).
	•	A energia óptica (J) está ligada ao brilho no topo da nuvem, captado pelo satélite geoestacionário.
	•	Diversos fatores (espessura da nuvem, umidade, altitude, polaridade) podem causar alta corrente com pouca emissão óptica ou vice-versa.

5.2. Comparação Física

O arquivo inclui a função analyze_physical_correlation(df, use_abs_current=True), que:
	1.	Toma as colunas de energia (glm_glm_energy) e corrente (entln_pico_corrente).
	2.	Converte em log10 para acomodar a ampla faixa de valores.
	3.	Calcula correlações estatísticas (Pearson e Spearman).
	4.	Retorna um indicador de quão relacionadas (ou não) essas grandezas aparecem no conjunto de raios pareados (GLM+ENTLN).

Em muitos casos, a correlação pode ser moderada ou fraca, pois um raio com grande corrente nem sempre resulta em grande emissão óptica captada pelo GLM (e vice-versa).

5.3. Uso Prático
	•	Meteorologia Operacional: Saber quais raios detectados pelo GLM correspondem a raios detectados pelo ENTLN pode ajudar a validar alertas de tempestade severa.
	•	Segurança e Pesquisa: A junção dos dados permite ver eventuais falhas de detecção em uma rede ou na outra, além de estudar a física dos raios em maior profundidade (corrente vs. luminosidade).
	•	Limitações:
	•	Precisão de localização do GLM (~8-10 km ou mais, dependendo do ângulo) e do ENTLN (depende da densidade de sensores).
	•	Erros de tempo ou distâncias podem causar “falsos pareamentos” ou “falhas de pareamento”.

6. Conclusão

O roteiro aqui documentado carrega CSVs de duas fontes (GLM e ENTLN), aplica tratamentos para unificar formatações (datas, colunas, escalas), correlaciona os dados com base em critérios de tempo e distância, e analisa a relação estatística (log de pico de corrente vs. log de energia).
	•	O resultado é um DataFrame que combina os dados pareados (full join), além de colunas _scaled para comparar visualmente a intensidade dos eventos.
	•	A análise de correlação (log scale) ajuda a identificar tendências ou ausência delas entre o pico de corrente medido no solo e a energia óptica captada por satélite.
	•	Ainda assim, não se trata de equivalência direta entre kA e J, e sim de observações complementares sobre o mesmo fenômeno de raios sob perspectivas diferentes (rede terrestre × sensor espacial).

Recomendação: Ajustar time_tolerance_s e distance_tolerance_km de acordo com a resolução e precisão de cada sensor, além de realizar testes de sensibilidade para ver quantos pareamentos efetivos ocorrem ou quantos possíveis pares são descartados.