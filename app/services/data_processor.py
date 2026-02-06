import pandas as pd
import numpy as np
from datetime import datetime
import logging
import re
from app.utils.regions import REGION_MAPPING, STATE_TO_REGION
from app.config import settings

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, df):
        self.original_df = df.copy()
        self.df = df.copy()
        self._clean_data()
        self._enhance_data()
    
    def _normalize_region(self, region: str) -> str:
        """Normaliza o nome da região para o formato padrão"""
        if not region or pd.isna(region):
            return "NÃO IDENTIFICADA"
        
        region_str = str(region).upper().strip()
        
        # Mapear todas as variações para o formato padrão
        region_mapping = {
            'CENTRO-OESTE': 'CENTRO_OESTE',
            'CENTRO OESTE': 'CENTRO_OESTE',
            'CENTRO  OESTE': 'CENTRO_OESTE',
            'CENTROOESTE': 'CENTRO_OESTE',
            'SUDESTE': 'SUDESTE',
            'SUL': 'SUL',
            'NORDESTE': 'NORDESTE',
            'NORTE': 'NORTE',
            'CENTRO_OESTE': 'CENTRO_OESTE'  # Já está correto
        }
        
        return region_mapping.get(region_str, region_str)
    
    def _clean_data(self):
        """Limpa e normaliza dados"""
        logger.info("Iniciando limpeza de dados...")
        initial_count = len(self.df)
        
        try:
            # Remover linhas completamente vazias
            self.df = self.df.dropna(how='all')
            
            # Normalizar nomes de colunas (garantir que existem)
            required_columns = ['MUNICIPIO', 'ESTADO', 'PRODUTO', 'PRECO_MEDIO_REVENDA']
            
            for col in required_columns:
                if col not in self.df.columns:
                    logger.error(f"Coluna obrigatória não encontrada: {col}")
                    logger.info(f"Colunas disponíveis: {list(self.df.columns)}")
                    raise ValueError(f"Coluna {col} não encontrada nos dados")
            
            # Converter tipos de dados
            self.df['PRECO_MEDIO_REVENDA'] = pd.to_numeric(
                self.df['PRECO_MEDIO_REVENDA'], errors='coerce'
            )
            
            if 'NUMERO_DE_POSTOS_PESQUISADOS' in self.df.columns:
                self.df['NUMERO_DE_POSTOS_PESQUISADOS'] = pd.to_numeric(
                    self.df['NUMERO_DE_POSTOS_PESQUISADOS'], errors='coerce'
                ).fillna(1).astype(int)
            else:
                self.df['NUMERO_DE_POSTOS_PESQUISADOS'] = 1
            
            # Remover registros com preço inválido
            self.df = self.df.dropna(subset=['PRECO_MEDIO_REVENDA'])
            self.df = self.df[self.df['PRECO_MEDIO_REVENDA'] > 0]
            
            # Normalizar strings
            self.df['MUNICIPIO'] = self.df['MUNICIPIO'].astype(str).str.upper().str.strip()
            self.df['ESTADO'] = self.df['ESTADO'].astype(str).str.upper().str.strip()
            self.df['PRODUTO'] = self.df['PRODUTO'].astype(str).str.upper().str.strip()
            
            # Normalizar REGIAO - corrigir "CENTRO OESTE" para "CENTRO_OESTE"
            if 'REGIAO' in self.df.columns:
                self.df['REGIAO'] = self.df['REGIAO'].astype(str).str.upper().str.strip()
                # Corrigir todas as variações para "CENTRO_OESTE" (COM UNDERLINE)
                self.df['REGIAO'] = self.df['REGIAO'].str.replace('CENTRO OESTE', 'CENTRO_OESTE')
                self.df['REGIAO'] = self.df['REGIAO'].str.replace('CENTRO  OESTE', 'CENTRO_OESTE')
                self.df['REGIAO'] = self.df['REGIAO'].str.replace('CENTRO-OESTE', 'CENTRO_OESTE')
                self.df['REGIAO'] = self.df['REGIAO'].str.replace('CENTROOESTE', 'CENTRO_OESTE')
            
            # Remover caracteres especiais e acentos
            self.df['MUNICIPIO'] = self.df['MUNICIPIO'].apply(self._normalize_text)
            self.df['PRODUTO'] = self.df['PRODUTO'].apply(self._normalize_text)
            
            # Converter nomes de estados completos para siglas
            def estado_para_sigla(estado_nome):
                estado_nome = str(estado_nome).upper().strip()
                
                mapeamento = {
                    'SAO PAULO': 'SP', 'SÃO PAULO': 'SP',
                    'RIO DE JANEIRO': 'RJ', 
                    'MINAS GERAIS': 'MG',
                    'ESPIRITO SANTO': 'ES', 'ESPÍRITO SANTO': 'ES',
                    'PARANA': 'PR', 'PARANÁ': 'PR',
                    'SANTA CATARINA': 'SC',
                    'RIO GRANDE DO SUL': 'RS',
                    'MATO GROSSO': 'MT',
                    'MATO GROSSO DO SUL': 'MS',
                    'GOIAS': 'GO', 'GOIÁS': 'GO',
                    'DISTRITO FEDERAL': 'DF',
                    'BAHIA': 'BA', 'BAÍA': 'BA',
                    'PERNAMBUCO': 'PE',
                    'CEARA': 'CE', 'CEARÁ': 'CE',
                    'MARANHAO': 'MA', 'MARANHÃO': 'MA',
                    'PIAUI': 'PI', 'PIAUÍ': 'PI',
                    'RIO GRANDE DO NORTE': 'RN',
                    'PARAIBA': 'PB', 'PARAÍBA': 'PB',
                    'ALAGOAS': 'AL',
                    'SERGIPE': 'SE',
                    'PARA': 'PA', 'PARÁ': 'PA',
                    'AMAZONAS': 'AM',
                    'ACRE': 'AC',
                    'RONDONIA': 'RO', 'RONDÔNIA': 'RO',
                    'RORAIMA': 'RR',
                    'AMAPA': 'AP', 'AMAPÁ': 'AP',
                    'TOCANTINS': 'TO'
                }
                
                return mapeamento.get(estado_nome, estado_nome)
            
            self.df['ESTADO_SIGLA'] = self.df['ESTADO'].apply(estado_para_sigla)
            logger.info(f"Estados convertidos. Exemplos: {self.df[['ESTADO', 'ESTADO_SIGLA']].head(10).to_dict('records')}")
            
            # Mapear regiões usando as siglas
            def get_region_from_state_sigla(sigla):
                sigla = str(sigla).upper().strip()
                
                region_mapping = {
                    'SP': 'SUDESTE', 'RJ': 'SUDESTE', 'MG': 'SUDESTE', 'ES': 'SUDESTE',
                    'PR': 'SUL', 'SC': 'SUL', 'RS': 'SUL',
                    'BA': 'NORDESTE', 'PE': 'NORDESTE', 'CE': 'NORDESTE', 'MA': 'NORDESTE',
                    'PI': 'NORDESTE', 'RN': 'NORDESTE', 'PB': 'NORDESTE', 'AL': 'NORDESTE', 'SE': 'NORDESTE',
                    'GO': 'CENTRO_OESTE', 'MT': 'CENTRO_OESTE', 'MS': 'CENTRO_OESTE', 'DF': 'CENTRO_OESTE',
                    'AM': 'NORTE', 'PA': 'NORTE', 'AC': 'NORTE', 'RO': 'NORTE', 'RR': 'NORTE', 'AP': 'NORTE', 'TO': 'NORTE'
                }
                
                return region_mapping.get(sigla, 'NÃO IDENTIFICADA')
            
            self.df['REGIAO'] = self.df['ESTADO_SIGLA'].apply(get_region_from_state_sigla)
            
            # Se já tínhamos REGIAO do arquivo, priorizar ela (mas garantir formatação)
            if 'REGIAO' in self.df.columns:
                # Corrigir qualquer inconsistência
                self.df['REGIAO'] = self.df['REGIAO'].str.upper().str.strip()
                self.df['REGIAO'] = self.df['REGIAO'].str.replace('CENTRO OESTE', 'CENTRO-OESTE')
                self.df['REGIAO'] = self.df['REGIAO'].str.replace('CENTRO  OESTE', 'CENTRO-OESTE')
            
            # Verificar se há regiões não identificadas
            regioes_nao_id = self.df[self.df['REGIAO'] == 'NÃO IDENTIFICADA']
            if len(regioes_nao_id) > 0:
                logger.warning(f"Regiões não identificadas para {len(regioes_nao_id)} registros")
                logger.warning(f"Estados problemáticos: {regioes_nao_id['ESTADO'].unique()}")
            
            # Filtrar produtos relevantes
            valid_products = [
                'GASOLINA', 'GASOLINA COMUM', 'GASOLINA ADITIVADA',
                'DIESEL', 'DIESEL S10', 'DIESEL S500', 
                'GNV', 'GAS NATURAL VEICULAR', 'ETANOL', 'ALCOOL'
            ]
            
            self.df = self.df[self.df['PRODUTO'].isin(valid_products)]

            
            # Consolidar tipos similares
            # Primeiro, normalizar os nomes dos produtos
            self.df['PRODUTO'] = self.df['PRODUTO'].astype(str).str.strip().str.upper()
            
            product_mapping = {
                'GASOLINA': 'GASOLINA',
                'GASOLINA COMUM': 'GASOLINA', 
                'GASOLINA ADITIVADA': 'GASOLINA',
                'ETANOL HIDRATADO': 'ETANOL',  
                'ETANOL': 'ETANOL',
                'ÓLEO DIESEL': 'DIESEL',  
                'ÓLEO DIESEL S10': 'DIESEL_S10',  
                'ÓLEO_DIESEL': 'DIESEL',  # AQUI!
                'ÓLEO_DIESEL_S10': 'DIESEL_S10',  # AQUI!
                'DIESEL': 'DIESEL',
                'DIESEL S10': 'DIESEL_S10',
                'GNV': 'GNV',
                'GÁS NATURAL VEICULAR': 'GNV'
            }
            # Criar coluna produto_consolidado
            self.df['PRODUTO_CONSOLIDADO'] = self.df['PRODUTO'].map(
                lambda x: product_mapping.get(x, x)
            )
            # Log para debug
            logger.info(f"DEBUG - Colunas após processamento: {list(self.df.columns)}")
            logger.info(f"DEBUG - Tipos das colunas: {self.df.dtypes.to_dict()}")
            logger.info(f"DEBUG - Primeiras linhas:")
            for i in range(min(3, len(self.df))):
                logger.info(f"  Linha {i}: PRODUTO={self.df.iloc[i]['PRODUTO']}, PRODUTO_CONSOLIDADO={self.df.iloc[i]['PRODUTO_CONSOLIDADO']}")
                
            # Log para debug
            unique_products = self.df['PRODUTO'].unique()
            unique_consolidated = self.df['PRODUTO_CONSOLIDADO'].unique()
            logger.info(f"Produtos originais: {list(unique_products)}")
            logger.info(f"Produtos consolidados: {list(unique_consolidated)}")
            
            # Remover duplicatas (mantendo o menor preço)
            self.df = self.df.sort_values('PRECO_MEDIO_REVENDA')
            self.df = self.df.drop_duplicates(
                subset=['MUNICIPIO', 'ESTADO', 'PRODUTO_CONSOLIDADO'],
                keep='first'
            )
            
            final_count = len(self.df)
            logger.info(
                f"Limpeza concluída: {final_count}/{initial_count} "
                f"registros válidos ({initial_count - final_count} removidos)"
            )
            
            # Log estatísticas
            logger.info(f"Municípios únicos: {self.df['MUNICIPIO'].nunique()}")
            logger.info(f"Estados únicos: {self.df['ESTADO'].nunique()}")
            logger.info(f"Produtos únicos: {self.df['PRODUTO_CONSOLIDADO'].unique()}")
            logger.info(f"Preço médio geral: R$ {self.df['PRECO_MEDIO_REVENDA'].mean():.2f}")
            
        except Exception as e:
            logger.error(f"Erro na limpeza de dados: {e}")
            raise
    
    def _enhance_data(self):
        """Adiciona dados enriquecidos"""
        try:
            # Adicionar coordenadas aproximadas (em produção, usar API de geolocalização)
            # Por enquanto, apenas estrutura para futuro
            self.df['LATITUDE'] = None
            self.df['LONGITUDE'] = None
            
            # Adicionar timestamp
            self.df['DATA_PROCESSAMENTO'] = datetime.now()
            
            # Calcular preço relativo à média nacional
            national_avg = self.df['PRECO_MEDIO_REVENDA'].mean()
            self.df['PRECO_RELATIVO'] = (
                self.df['PRECO_MEDIO_REVENDA'] / national_avg * 100
            )
            
            # Classificar por faixa de preço
            bins = [0, 4.5, 5.0, 5.5, 6.0, float('inf')]
            labels = ['MUITO BAIXO', 'BAIXO', 'MEDIO', 'ALTO', 'MUITO ALTO']
            self.df['FAIXA_PRECO'] = pd.cut(
                self.df['PRECO_MEDIO_REVENDA'], bins=bins, labels=labels
            )
            
            logger.info("Dados enriquecidos com sucesso")
            
        except Exception as e:
            logger.warning(f"Erro ao enriquecer dados: {e}")
    
    def _normalize_text(self, text):
        """Normaliza texto removendo acentos e caracteres especiais"""
        if not isinstance(text, str):
            return text
        
        # Remover acentos
        import unicodedata
        text = unicodedata.normalize('NFKD', text)
        text = text.encode('ASCII', 'ignore').decode('ASCII')
        
        # Remover caracteres especiais exceto espaço
        text = re.sub(r'[^A-Z0-9\s]', '', text)
        
        return text.strip()
    
    def get_best_price_by_fuel(self, fuel_type: str):
        """Retorna melhor preço por tipo de combustível"""
        fuel_type_upper = fuel_type.upper()
        
        if fuel_type_upper == 'DIESEL_S10':
            fuel_filter = 'DIESEL_S10'
        else:
            fuel_filter = fuel_type_upper
        
        fuel_df = self.df[self.df['PRODUTO_CONSOLIDADO'] == fuel_filter]
        
        if fuel_df.empty:
            logger.warning(f"Nenhum dado encontrado para {fuel_type}")
            return None
        
        best_idx = fuel_df['PRECO_MEDIO_REVENDA'].idxmin()
        best = fuel_df.loc[best_idx]
        
        # Normalizar região
        region = self._normalize_region(best['REGIAO'])
        
        # Usar sigla do estado se disponível
        estado = best['ESTADO_SIGLA'] if 'ESTADO_SIGLA' in best else best['ESTADO']
        
        # Calcular latitude/longitude aproximada
        coords = self._estimate_coordinates(best['MUNICIPIO'], estado)
        
        return {
            'price': float(best['PRECO_MEDIO_REVENDA']),
            'city': best['MUNICIPIO'],
            'state': str(estado),
            'region': region,  # Já normalizada
            'fuel_type': fuel_type.lower(),
            'stations_count': int(best['NUMERO_DE_POSTOS_PESQUISADOS']),
            'latitude': coords['latitude'],
            'longitude': coords['longitude'],
            'price_band': best['FAIXA_PRECO'] if 'FAIXA_PRECO' in best else 'MEDIO'
        }
        
    def get_ranking(self, fuel_type: str, limit: int = 10):
        """Ranking dos municípios mais baratos"""
        fuel_type_upper = fuel_type.upper()
        
        if fuel_type_upper == 'DIESEL_S10':
            fuel_filter = 'DIESEL_S10'
        else:
            fuel_filter = fuel_type_upper
        
        fuel_df = self.df[self.df['PRODUTO_CONSOLIDADO'] == fuel_filter]
        
        if fuel_df.empty:
            return []
        
        # Agrupar por município (média de preços se houver múltiplos registros)
        grouped = fuel_df.groupby(['MUNICIPIO', 'ESTADO_SIGLA', 'REGIAO']).agg({
            'PRECO_MEDIO_REVENDA': 'mean',
            'NUMERO_DE_POSTOS_PESQUISADOS': 'sum'
        }).reset_index()
        
        # Ordenar por preço
        grouped = grouped.sort_values('PRECO_MEDIO_REVENDA')
        
        # Limitar resultados
        ranked = grouped.head(limit)
        
        ranking = []
        for i, (_, row) in enumerate(ranked.iterrows()):
            coords = self._estimate_coordinates(row['MUNICIPIO'], row['ESTADO_SIGLA'])
            
            ranking.append({
                'rank': i + 1,
                'city': row['MUNICIPIO'],
                'state': row['ESTADO_SIGLA'],
                'region': self._normalize_region(row['REGIAO']),  # Normalizar região
                'price': float(row['PRECO_MEDIO_REVENDA']),
                'stations': int(row['NUMERO_DE_POSTOS_PESQUISADOS']),
                'latitude': coords['latitude'],
                'longitude': coords['longitude']
            })
        
        return ranking
    
    def get_region_stats(self):
        """Estatísticas agregadas por região"""
        if self.df.empty:
            return []
        
        region_stats = []
        
        for region in self.df['REGIAO'].unique():
            region_df = self.df[self.df['REGIAO'] == region]
            
            # Garantir que a região está no formato correto
            region_normalized = str(region).upper().strip()
            region_normalized = region_normalized.replace('CENTRO-OESTE', 'CENTRO_OESTE')
            region_normalized = region_normalized.replace('CENTRO OESTE', 'CENTRO_OESTE')
            
            # Para cada tipo de combustível consolidado
            for fuel in ['GASOLINA', 'DIESEL', 'DIESEL_S10', 'GNV', 'ETANOL']:
                fuel_df = region_df[region_df['PRODUTO_CONSOLIDADO'] == fuel]
                
                if not fuel_df.empty:
                    region_stats.append({
                        'region': region_normalized,
                        'fuel_type': fuel.lower(),
                        'avg_price': float(fuel_df['PRECO_MEDIO_REVENDA'].mean()),
                        'min_price': float(fuel_df['PRECO_MEDIO_REVENDA'].min()),
                        'max_price': float(fuel_df['PRECO_MEDIO_REVENDA'].max()),
                        'city_count': fuel_df['MUNICIPIO'].nunique(),
                        'stations_count': int(fuel_df['NUMERO_DE_POSTOS_PESQUISADOS'].sum()),
                        'price_std': float(fuel_df['PRECO_MEDIO_REVENDA'].std())
                    })
        
        return region_stats
    
    def get_city_comparison(self, cities: list):
        """Comparação detalhada entre cidades"""
        results = []
        
        for city in cities:
            city_upper = city.upper()
            city_df = self.df[self.df['MUNICIPIO'] == city_upper]
            
            if city_df.empty:
                logger.warning(f"Cidade não encontrada: {city}")
                continue
            
            # Estatísticas por tipo de combustível
            fuels_data = {}
            for fuel in ['GASOLINA', 'DIESEL', 'DIESEL_S10', 'GNV']:
                fuel_df = city_df[city_df['PRODUTO_CONSOLIDADO'] == fuel]
                
                if not fuel_df.empty:
                    fuels_data[fuel.lower()] = {
                        'avg': float(fuel_df['PRECO_MEDIO_REVENDA'].mean()),
                        'min': float(fuel_df['PRECO_MEDIO_REVENDA'].min()),
                        'max': float(fuel_df['PRECO_MEDIO_REVENDA'].max()),
                        'stations': int(fuel_df['NUMERO_DE_POSTOS_PESQUISADOS'].sum()),
                        'std': float(fuel_df['PRECO_MEDIO_REVENDA'].std())
                    }
            
            # Se não encontrou dados para nenhum combustível, pular
            if not fuels_data:
                continue
            
            # Calcular coordenadas
            coords = self._estimate_coordinates(
                city_df.iloc[0]['MUNICIPIO'],
                city_df.iloc[0]['ESTADO']
            )
            
            results.append({
                'city': city_upper,
                'state': city_df.iloc[0]['ESTADO'],
                'region': city_df.iloc[0]['REGIAO'],
                'fuels': fuels_data,
                'overall_stats': {
                    'total_stations': int(city_df['NUMERO_DE_POSTOS_PESQUISADOS'].sum()),
                    'city_count': city_df['MUNICIPIO'].nunique(),
                    'avg_price_all': float(city_df['PRECO_MEDIO_REVENDA'].mean())
                },
                'coordinates': coords
            })
        
        return results
    
    def get_trend_analysis(self, fuel_type: str, days_back: int = 90):
        """Análise de tendência temporal"""
        fuel_type_upper = fuel_type.upper()
        
        if fuel_type_upper == 'DIESEL_S10':
            fuel_filter = 'DIESEL_S10'
        else:
            fuel_filter = fuel_type_upper
        
        fuel_df = self.df[self.df['PRODUTO_CONSOLIDADO'] == fuel_filter]
        
        if fuel_df.empty:
            return None
        
        # Estatísticas básicas
        prices = fuel_df['PRECO_MEDIO_REVENDA'].values
        current_price = float(np.mean(prices))
        price_std = float(np.std(prices))
        volatility = price_std / current_price if current_price > 0 else 0
        
        # Análise de distribuição
        skewness = float(
            (np.mean(prices) - np.median(prices)) / price_std 
            if price_std > 0 else 0
        )
        
        # Determinar tendência baseada na assimetria
        if skewness > 0.1:
            trend = "alta"
            trend_strength = min(100, abs(skewness) * 100)
        elif skewness < -0.1:
            trend = "baixa"
            trend_strength = min(100, abs(skewness) * 100)
        else:
            trend = "estavel"
            trend_strength = 0
        
        # Gerar recomendação
        recommendation, reason = self._generate_trend_recommendation(
            current_price, volatility, trend, trend_strength
        )
        
        # Calcular nível de confiança
        confidence = self._calculate_confidence_level(len(prices), volatility)
        
        return {
            'current_price': round(current_price, 3),
            'volatility': round(volatility, 4),
            'recommendation': recommendation,
            'reason': reason,
            'analysis_date': datetime.now(),
            'trend_indicator': round(skewness * 50, 2),  # -50 a 50
            'confidence_level': round(confidence, 1),
            'trend_direction': trend,
            'trend_strength': round(trend_strength, 1),
            'price_range': {
                'min': float(np.min(prices)),
                'max': float(np.max(prices)),
                'q1': float(np.percentile(prices, 25)),
                'median': float(np.median(prices)),
                'q3': float(np.percentile(prices, 75))
            }
        }
    
    def _estimate_coordinates(self, city: str, state: str):
        """Estima coordenadas geográficas (em produção, usar API real)"""
        # Converter estado para sigla se necessário
        state_str = str(state).upper().strip()
        
        # Se estado está por extenso, converter para sigla
        estado_para_sigla = {
            'SAO PAULO': 'SP', 'SÃO PAULO': 'SP',
            'RIO DE JANEIRO': 'RJ', 
            'MINAS GERAIS': 'MG',
            'ESPIRITO SANTO': 'ES', 'ESPÍRITO SANTO': 'ES',
            'PARANA': 'PR', 'PARANÁ': 'PR',
            'SANTA CATARINA': 'SC',
            'RIO GRANDE DO SUL': 'RS',
            'MATO GROSSO': 'MT',
            'MATO GROSSO DO SUL': 'MS',
            'GOIAS': 'GO', 'GOIÁS': 'GO',
            'DISTRITO FEDERAL': 'DF',
            'BAHIA': 'BA',
            'PERNAMBUCO': 'PE',
            'CEARA': 'CE', 'CEARÁ': 'CE',
            'MARANHAO': 'MA', 'MARANHÃO': 'MA',
            'PIAUI': 'PI', 'PIAUÍ': 'PI',
            'RIO GRANDE DO NORTE': 'RN',
            'PARAIBA': 'PB', 'PARAÍBA': 'PB',
            'ALAGOAS': 'AL',
            'SERGIPE': 'SE',
            'PARA': 'PA', 'PARÁ': 'PA',
            'AMAZONAS': 'AM',
            'ACRE': 'AC',
            'RONDONIA': 'RO', 'RONDÔNIA': 'RO',
            'RORAIMA': 'RR',
            'AMAPA': 'AP', 'AMAPÁ': 'AP',
            'TOCANTINS': 'TO'
        }
        
        # Verificar se o estado está no mapeamento
        if state_str in estado_para_sigla:
            state_sigla = estado_para_sigla[state_str]
            logger.debug(f"Convertido estado '{state_str}' para sigla '{state_sigla}'")
        else:
            state_sigla = state_str  # Já deve ser sigla
        
        # Coordenadas aproximadas das capitais (usando SIGLAS)
        capital_coords = {
            'SP': (-23.5505, -46.6333),  # São Paulo
            'RJ': (-22.9068, -43.1729),  # Rio de Janeiro
            'MG': (-19.9167, -43.9345),  # Belo Horizonte
            'RS': (-30.0331, -51.2300),  # Porto Alegre
            'PR': (-25.4284, -49.2733),  # Curitiba
            'SC': (-27.5954, -48.5480),  # Florianópolis
            'DF': (-15.7942, -47.8822),  # Brasília
            'GO': (-16.6869, -49.2648),  # Goiânia
            'MT': (-15.6010, -56.0974),  # Cuiabá
            'MS': (-20.4697, -54.6201),  # Campo Grande
            'BA': (-12.9714, -38.5014),  # Salvador
            'PE': (-8.0476, -34.8770),  # Recife
            'CE': (-3.7172, -38.5433),  # Fortaleza
            'RN': (-5.7945, -35.2110),  # Natal
            'PB': (-7.1195, -34.8450),  # João Pessoa
            'AL': (-9.6658, -35.7350),  # Maceió
            'SE': (-10.9472, -37.0731),  # Aracaju
            'MA': (-2.5387, -44.2830),  # São Luís
            'PI': (-5.0892, -42.8016),  # Teresina
            'PA': (-1.4558, -48.4902),  # Belém
            'AM': (-3.1190, -60.0217),  # Manaus
            'AC': (-9.9747, -67.8100),  # Rio Branco
            'RO': (-8.7612, -63.9039),  # Porto Velho
            'RR': (2.8195, -60.6714),   # Boa Vista
            'AP': (0.0349, -51.0664),   # Macapá
            'TO': (-10.1844, -48.3336)  # Palmas
        }
        
        if state_sigla in capital_coords:
            lat, lon = capital_coords[state_sigla]
            # Adicionar pequena variação baseada no nome da cidade
            import hashlib
            city_hash = int(hashlib.md5(city.encode()).hexdigest()[:8], 16)
            lat_variation = (city_hash % 1000 - 500) / 10000  # +/- 0.05 graus
            lon_variation = ((city_hash >> 10) % 1000 - 500) / 10000
            
            return {
                'latitude': round(lat + lat_variation, 6),
                'longitude': round(lon + lon_variation, 6)
            }
        
        logger.warning(f"Estado '{state}' (sigla: '{state_sigla}') não encontrado no mapeamento de coordenadas")
        return {'latitude': None, 'longitude': None}
    
    def _generate_trend_recommendation(self, price, volatility, trend, strength):
        """Gera recomendação baseada na análise de tendência"""
        
        if volatility > 0.1:
            if trend == "alta":
                return "abastecer_agora", (
                    f"Alta volatilidade ({volatility*100:.1f}%) com tendência de alta. "
                    f"Risco significativo de aumento nos preços."
                )
            else:
                return "abastecer_agora", (
                    f"Alta volatilidade ({volatility*100:.1f}%). "
                    f"Recomenda-se abastecer devido à instabilidade dos preços."
                )
        
        elif volatility > 0.05:
            if trend == "alta" and strength > 30:
                return "abastecer_agora", (
                    f"Tendência de alta moderada com volatilidade média. "
                    f"Condições favoráveis para abastecimento imediato."
                )
            elif trend == "baixa" and strength > 30:
                return "pode_esperar", (
                    f"Tendência de baixa detectada. "
                    f"Pode aguardar por possíveis reduções nos preços."
                )
            else:
                return "pode_esperar", "Situação neutra com volatilidade moderada."
        
        else:
            if trend == "alta" and strength > 50:
                return "abastecer_agora", (
                    f"Tendência consistente de alta com baixa volatilidade. "
                    f"Preços podem subir gradualmente."
                )
            elif trend == "baixa" and strength > 50:
                return "pode_esperar", (
                    f"Tendência consistente de baixa. "
                    f"Oportunidade para economizar aguardando."
                )
            else:
                return "pode_esperar", "Preços estáveis sem tendência definida."
    
    def _calculate_confidence_level(self, sample_size, volatility):
        """Calcula nível de confiança da análise"""
        # Baseado no tamanho da amostra
        sample_score = min(100, (sample_size / 500) * 100)
        
        # Penalidade por alta volatilidade
        volatility_penalty = volatility * 150
        
        # Penalidade por amostra pequena
        size_penalty = max(0, (500 - sample_size) / 500 * 30)
        
        confidence = sample_score - volatility_penalty - size_penalty
        
        return max(0, min(100, confidence))
    
    def get_summary_stats(self):
        """Retorna estatísticas resumidas do dataset"""
        if self.df.empty:
            return {}
        
        total_stations = int(self.df['NUMERO_DE_POSTOS_PESQUISADOS'].sum())
        
        stats = {
            'total_records': len(self.df),
            'unique_municipalities': self.df['MUNICIPIO'].nunique(),
            'unique_states': self.df['ESTADO'].nunique(),
            'total_stations': total_stations,
            'price_statistics': {
                'min': float(self.df['PRECO_MEDIO_REVENDA'].min()),
                'max': float(self.df['PRECO_MEDIO_REVENDA'].max()),
                'mean': float(self.df['PRECO_MEDIO_REVENDA'].mean()),
                'median': float(self.df['PRECO_MEDIO_REVENDA'].median()),
                'std': float(self.df['PRECO_MEDIO_REVENDA'].std())
            },
            'fuel_type_distribution': self.df['PRODUTO_CONSOLIDADO'].value_counts().to_dict(),
            'region_distribution': self.df['REGIAO'].value_counts().to_dict(),
            'data_quality': {
                'missing_prices': self.original_df['PRECO_MEDIO_REVENDA'].isna().sum(),
                'zero_prices': (self.original_df['PRECO_MEDIO_REVENDA'] == 0).sum(),
                'negative_prices': (self.original_df['PRECO_MEDIO_REVENDA'] < 0).sum()
            }
        }
        
        return stats
