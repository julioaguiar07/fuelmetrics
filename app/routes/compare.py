from fastapi import APIRouter, Query, HTTPException
from typing import List
import logging
from datetime import datetime
from app.services.anp_downloader import ANPDownloader
from app.services.data_processor import DataProcessor
from app.services.cache_manager import cache
from app.models.schemas import CityComparison, FuelType
from app.utils.column_helper import get_column_mapping, normalize_city_name

router = APIRouter()
logger = logging.getLogger(__name__)

def get_processor():
    """Obter processador de dados"""
    from app.routes.today import get_processor as get_global_processor
    return get_global_processor()

# No método compare_cities, atualize após obter o processor:

@router.get("/cities", response_model=List[CityComparison])
async def compare_cities(
    cities: str = Query(..., description="Lista de cidades separadas por vírgula"),
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Compara preços entre múltiplas cidades"""
    try:
        # Parse da lista de cidades
        city_list = [city.strip() for city in cities.split(',')]
        
        if len(city_list) < 2:
            raise HTTPException(
                status_code=400,
                detail="Forneça pelo menos duas cidades para comparação"
            )
        
        processor = get_processor()
        df = processor.df
        
        # USAR DADOS RECENTES - ADICIONE ESTA LINHA
        from app.utils.column_helper import get_latest_data
        df = get_latest_data(df)
        
        # Usar helper para mapeamento correto
        col_map = get_column_mapping(df)
        
        # VERIFICAÇÃO CRÍTICA: Log das colunas mapeadas
        logger.info(f"DEBUG - Comparando cidades: {city_list}")
        logger.info(f"DEBUG - Colunas mapeadas: {col_map}")
        
        # Verificar coluna de produto consolidado
        produto_col = col_map.get('produto_consolidado', col_map.get('produto', 'PRODUTO_CONSOLIDADO'))
        logger.info(f"DEBUG - Usando coluna de produto: {produto_col}")
        
        if produto_col in df.columns:
            # Normalizar os nomes dos produtos
            from app.utils.column_helper import normalize_product_name
            df['PRODUTO_NORMALIZADO'] = df[produto_col].apply(normalize_product_name)
            logger.info(f"DEBUG - Produtos normalizados únicos: {df['PRODUTO_NORMALIZADO'].unique()[:10]}")
        
        comparisons = []
        
        for city in city_list:
            # Normalizar nome da cidade
            city_normalized = normalize_city_name(city)
            
            # Filtrar dados da cidade
            city_data = df[
                df[col_map['municipio']].astype(str).apply(normalize_city_name) == city_normalized
            ]
            
            if city_data.empty:
                logger.warning(f"Cidade não encontrada: {city} (normalizado: {city_normalized})")
                continue
            
            # Filtrar por tipo de combustível NORMALIZADO
            fuel_type_normalized = fuel_type.value.upper()
            
            # Usar coluna normalizada se existir, senão usar a original
            if 'PRODUTO_NORMALIZADO' in city_data.columns:
                fuel_data = city_data[
                    city_data['PRODUTO_NORMALIZADO'] == fuel_type_normalized
                ]
            else:
                fuel_data = city_data[
                    city_data[produto_col].astype(str).str.upper() == fuel_type_normalized
                ]
            
            # Se vazio, tentar variações para diesel
            if fuel_data.empty and fuel_type.value in ['diesel', 'diesel_s10']:
                if fuel_type.value == 'diesel':
                    if 'PRODUTO_NORMALIZADO' in city_data.columns:
                        fuel_data = city_data[city_data['PRODUTO_NORMALIZADO'] == 'DIESEL']
                    else:
                        fuel_data = city_data[city_data[produto_col].astype(str).str.contains('DIESEL', case=False, na=False)]
                elif fuel_type.value == 'diesel_s10':
                    if 'PRODUTO_NORMALIZADO' in city_data.columns:
                        fuel_data = city_data[city_data['PRODUTO_NORMALIZADO'] == 'DIESEL_S10']
                    else:
                        fuel_data = city_data[city_data[produto_col].astype(str).str.contains('DIESEL_S10', case=False, na=False)]
            
            if fuel_data.empty:
                logger.warning(f"Nenhum dado de {fuel_type.value} para {city}")
                if 'PRODUTO_NORMALIZADO' in city_data.columns:
                    available_fuels = city_data['PRODUTO_NORMALIZADO'].unique()
                else:
                    available_fuels = city_data[produto_col].unique()
                logger.warning(f"Valores disponíveis: {available_fuels}")
                continue
            
            # Obter colunas corretas
            preco_col = col_map.get('preco_medio_revenda', 'PRECO_MEDIO_REVENDA')
            postos_col = col_map.get('numero_de_postos_pesquisados', 'NUMERO_DE_POSTOS_PESQUISADOS')
            estado_col = col_map.get('estado', 'ESTADO')
            regiao_col = col_map.get('regiao', 'REGIAO')
            
            # Garantir que temos valores válidos
            if fuel_data.empty or preco_col not in fuel_data.columns:
                logger.error(f"Dados inválidos para {city}: DataFrame vazio ou sem coluna de preço")
                continue
            
            try:
                # Calcular estatísticas com verificações de segurança
                avg_price = float(fuel_data[preco_col].mean())
                min_price = float(fuel_data[preco_col].min())
                max_price = float(fuel_data[preco_col].max())
                
                # Garantir que temos coluna de postos
                if postos_col in fuel_data.columns:
                    total_stations = int(fuel_data[postos_col].sum())
                else:
                    total_stations = len(fuel_data)
                
                # Log para debug
                logger.info(f"DEBUG - {city}: {len(fuel_data)} registros, preço médio: {avg_price}")
                
                # Criar objeto de combustível
                fuels_data = {
                    fuel_type.value: {
                        'avg': round(avg_price, 3),
                        'min': round(min_price, 3),
                        'max': round(max_price, 3),
                        'stations': total_stations,
                        'std': float(fuel_data[preco_col].std()) if len(fuel_data) > 1 else 0.0
                    }
                }
                
                # Obter estado e região
                estado = ""
                regiao = ""
                if not fuel_data.empty:
                    if estado_col in fuel_data.columns:
                        estado = str(fuel_data.iloc[0][estado_col])
                    if regiao_col in fuel_data.columns:
                        regiao = str(fuel_data.iloc[0][regiao_col])
                
                # Criar objeto de comparação
                comparison = CityComparison(
                    city=city_normalized,
                    state=estado,
                    region=regiao,
                    fuels=fuels_data,
                    overall_stats={
                        'total_stations': total_stations,
                        'city_count': 1
                    }
                )
                comparisons.append(comparison)
                
            except Exception as calc_error:
                logger.error(f"Erro ao calcular estatísticas para {city}: {calc_error}")
                continue
        
        if len(comparisons) < 2:
            raise HTTPException(
                status_code=404,
                detail="Dados insuficientes para comparação"
            )
        
        # Determinar recomendação
        comparisons = _add_recommendations(comparisons, fuel_type.value)
        
        return comparisons
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /cities: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")

@router.get("/recommendation")
async def get_recommendation(
    cities: str = Query(..., description="Lista de cidades separadas por vírgula"),
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Retorna recomendação de qual cidade escolher"""
    try:
        # Obter comparação
        comparisons = await compare_cities(cities, fuel_type)
        
        # Encontrar melhor opção
        best_option = None
        best_price = float('inf')
        
        for comp in comparisons:
            avg_price = comp.fuels[fuel_type.value]['avg']
            if avg_price < best_price:
                best_price = avg_price
                best_option = comp
        
        # Calcular economia
        worst_option = max(comparisons, key=lambda x: x.fuels[fuel_type.value]['avg'])
        savings_per_liter = worst_option.fuels[fuel_type.value]['avg'] - best_price
        savings_percentage = (savings_per_liter / worst_option.fuels[fuel_type.value]['avg']) * 100
        
        return {
            'best_option': {
                'city': best_option.city,
                'state': best_option.state,
                'price': best_price
            },
            'worst_option': {
                'city': worst_option.city,
                'state': worst_option.state,
                'price': worst_option.fuels[fuel_type.value]['avg']
            },
            'savings': {
                'per_liter': round(savings_per_liter, 3),
                'percentage': round(savings_percentage, 2),
                'per_50_liters': round(savings_per_liter * 50, 2)
            },
            'analysis_date': datetime.now().isoformat(),
            'recommendation': f"Abasteça em {best_option.city}, {best_option.state}",
            'reason': f"Economia de R$ {savings_per_liter:.2f} por litro ({savings_percentage:.1f}%)"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /recommendation: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

def _add_recommendations(comparisons: List[CityComparison], fuel_type: str) -> List[CityComparison]:
    """Adiciona recomendações às comparações"""
    if not comparisons:
        return comparisons
    
    # Encontrar melhor preço
    best_price = min(comp.fuels[fuel_type]['avg'] for comp in comparisons)
    
    for comp in comparisons:
        comp.recommendation = (
            "recommended" if comp.fuels[fuel_type]['avg'] == best_price 
            else "not_recommended"
        )
    
    return comparisons

@router.get("/nearby")
async def find_nearby_cities(
    city: str = Query(..., description="Cidade base"),
    max_distance_km: float = Query(50, ge=1, le=500, description="Distância máxima em km"),
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Encontra cidades próximas para comparação"""
    try:
        processor = get_processor()
        
        # Obter coordenadas da cidade (simplificado)
        # Em produção, usar API de geolocalização
        city_data = processor.df[processor.df['municipio'] == city.upper()]
        
        if city_data.empty:
            raise HTTPException(status_code=404, detail="Cidade não encontrada")
        
        # Listar todas as cidades do mesmo estado (simplificado)
        state = city_data.iloc[0]['estado']
        state_cities = processor.df[
            (processor.df['estado'] == state) & 
            (processor.df['produto_consolidado'] == fuel_type.value.upper()) &
            (processor.df['municipio'] != city.upper())
        ]
        
        # Agrupar por cidade
        grouped = state_cities.groupby(['municipio']).agg({
            'preco_medio_revenda': 'mean',
            'numero_de_postos_pesquisados': 'sum'
        }).reset_index()
        
        # Ordenar por preço
        grouped = grouped.sort_values('preco_medio_revenda')
        
        # Limitar resultados
        grouped = grouped.head(10)
        
        return [
            {
                'city': row['municipio'],
                'state': state,
                'avg_price': float(row['preco_medio_revenda']),
                'stations': int(row['numero_de_postos_pesquisados']),
                'distance_estimate': "no mesmo estado",  # Em produção: calcular distância real
                'savings_vs_base': float(city_data['preco_medio_revenda'].mean() - row['preco_medio_revenda'])
            }
            for _, row in grouped.iterrows()
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /nearby: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")
