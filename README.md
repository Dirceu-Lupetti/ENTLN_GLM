README

Contexto:
Este material descreve um processo de integração e análise de descargas atmosféricas (raios) a partir de duas fontes: 
1. ENTLN (Earth Networks Total Lightning Network), que fornece dados de pico de corrente em solo.  
2. GLM (Geostationary Lightning Mapper), instrumento a bordo dos satélites GOES que detecta emissões ópticas no topo das nuvens.

O objetivo é unificar, filtrar e correlacionar as medições, verificando coincidências e discrepâncias para fim de análise meteorológica e física dos raios.

Estrutura do Documento:
• Introdução às bases de dados e sensores (ENTLN e GLM)  
• Grandezas físicas observadas (pico de corrente × energia óptica)  
• Método de comparação e junção (Full Join)  
• Procedimentos de limpeza, conversão de formatos e normalização  
• Análise de correlação estatística e principais implicações

1. Bases de Dados

1.1. ENTLN (Earth Networks Total Lightning Network)  
• Rede terrestre que detecta raios em solo, fornecendo localização (entln_lat, entln_lon), horário (datahora) e o pico de corrente (pico_corrente).  
• Pode mensurar tanto raios nuvem-solo (CG) quanto intra-nuvem (IC).  

1.2. GLM (Geostationary Lightning Mapper)  
• Sensor óptico embarcado nos satélites GOES-16/17, com localização (glm_lat, glm_lon), momento (glm_time) e energia óptica detectada (glm_energy).  
• Mede emissões luminosas no topo da nuvem, influenciadas por espessura, geometria do raio e condições de iluminação.

2. Grandezas Físicas

• ENTLN: Pico de Corrente (kA), indicador elétrico do pulso, com polaridade.  
• GLM: Energia Óptica (J), medida do brilho no topo da nuvem.  
Não há conversão direta entre pico de corrente e energia óptica, pois são efeitos diferentes do mesmo fenômeno.

3. Comparação e Junção (Full Join)

• Tolerância Temporal: associa eventos que ocorreram em um intervalo, por exemplo, ±30 segundos.  
• Tolerância Espacial: verifica a proximidade (p. ex. até 10 km) via fórmula de Haversine.  
• Full Join: além dos pares combinados, preserva registros únicos de cada base (GLM ou ENTLN) não associados.

4. Tratamento

4.1. Padronização de Datas  
• Conversão (try_parsing_date) de formatos diversos para datetime (UTC) e arredondamento a segundos.  
• process_in_batches: processamento em lotes para grandes volumes de dados.

4.2. Filtro Temporal Opcional  
• filtrar_por_datahora: seleção por dia ou hora específicos.

4.3. Padronização de Colunas  
• ENTLN → entln_lat, entln_lon, entln_pico_corrente etc.  
• GLM → glm_lat, glm_lon, glm_energy, glm_time etc.

4.4. Distância Haversine  
• haversine_distance faz o cálculo aproximado entre duas coordenadas (lat, lon).

4.5. Associação Final (correlate_glm_entln)  
• Procura eventos ENTLN em torno de cada GLM (±time_tolerance_s, até distance_tolerance_km).  
• Produz pares casados, eventos “GLM only” e “ENTLN only” para o resultado final.

4.6. Normalização  
• add_intensity_scales gera colunas em [0,1] (Min-Max) para comparar visualmente pico de corrente e energia óptica.

5. Análise e Implicações

5.1. Grandezas Diferentes  
• Pico de corrente (kA) e energia óptica (J) são parâmetros distintos.  
• As correlações podem variar devido a fatores como espessura da nuvem, polaridade do raio e geometria.

5.2. Correlação Estatística  
• analyze_physical_correlation calcula coeficientes (Pearson, Spearman) em escala log10.  
• Serve para avaliar tendências entre eventos pareados, ainda que a relação possa ser apenas moderada ou fraca.

5.3. Aplicações  
• Meteorologia Operacional: validação e aperfeiçoamento de alertas de tempestade.  
• Segurança e Pesquisa: identificação de falhas de detecção, estudo da física dos raios e análise aprofundada de fenômenos de descarga elétrica × emissão de luz.

6. Conclusão

Este roteiro abrange o carregamento e padronização de dados ENTLN e GLM, sua união baseada em proximidade espaciotemporal e a análise de correlação entre pico de corrente e energia óptica.  
A resultante fusão (Full Join) exibe eventos pareados ou isolados, acompanhados de escalas normalizadas para visualização.  
A correlação estatística (log scale) sinaliza possíveis tendências, embora não exista equivalência direta entre kA e J.  
O ajuste de parâmetros (time_tolerance_s, distance_tolerance_km) deve ser feito conforme a precisão de cada sensor e as necessidades do estudo.

