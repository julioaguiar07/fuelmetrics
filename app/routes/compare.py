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
        
        # USAR DADOS RECENTES
        from app.utils.column_helper import get_latest_data
        # df = get_latest_data(df)
        
        # Usar helper para mapeamento correto
        col_map = get_column_mapping(df)
        
        # DEBUG: Verificar dados
        logger.info(f"DEBUG - Comparando cidades: {city_list}")
        logger.info(f"DEBUG - Fuel type: {fuel_type.value}")
        logger.info(f"DEBUG - Colunas mapeadas: {col_map}")
        
        # Obter colunas
        municipio_col = col_map.get('municipio', 'MUNICIPIO')
        produto_col = col_map.get('produto_consolidado', 'PRODUTO_CONSOLIDADO')
        preco_col = col_map.get('preco_medio_revenda', 'PRECO_MEDIO_REVENDA')
        postos_col = col_map.get('numero_de_postos_pesquisados', 'NUMERO_DE_POSTOS_PESQUISADOS')
        estado_col = col_map.get('estado', 'ESTADO')
        regiao_col = col_map.get('regiao', 'REGIAO')
        
        logger.info(f"DEBUG - Coluna município: {municipio_col}")
        logger.info(f"DEBUG - Coluna produto: {produto_col}")
        logger.info(f"DEBUG - Coluna preço: {preco_col}")
        
        # Verificar todos os produtos disponíveis no dataset
        if produto_col in df.columns:
            all_products = df[produto_col].unique()
            logger.info(f"DEBUG - Todos os produtos disponíveis no dataset: {all_products}")
        
        comparisons = []
        found_cities = []
        
        for city in city_list:
            city_upper = city.upper().strip()
            logger.info(f"DEBUG ===== Buscando cidade: '{city}' -> '{city_upper}' =====")
            
            # BUSCAR CIDADE - Múltiplas estratégias
            city_data = None
            
            # 1. Match exato (MAIÚSCULAS)
            mask_exact = df[municipio_col].astype(str).str.upper() == city_upper
            if mask_exact.any():
                city_data = df[mask_exact]
                logger.info(f"DEBUG - Encontrado por match exato: {city_upper}")
            
            # 2. Se não encontrou, tentar normalização
            if city_data is None or city_data.empty:
                from app.utils.column_helper import normalize_city_name
                city_normalized = normalize_city_name(city)
                mask_normalized = df[municipio_col].apply(normalize_city_name) == city_normalized
                if mask_normalized.any():
                    city_data = df[mask_normalized]
                    logger.info(f"DEBUG - Encontrado por normalização: {city_normalized}")
            
            # 3. Se ainda não encontrou, tentar busca parcial
            if city_data is None or city_data.empty:
                mask_partial = df[municipio_col].astype(str).str.contains(city_upper, case=False, na=False)
                if mask_partial.any():
                    city_data = df[mask_partial]
                    logger.info(f"DEBUG - Encontrado por busca parcial: {city_upper}")
            
            if city_data is None or city_data.empty:
                logger.warning(f"Cidade não encontrada: {city}")
                continue
            
            found_cities.append(city_upper)
            logger.info(f"DEBUG - {city}: encontrados {len(city_data)} registros")
            
            # Verificar se temos coluna de produto
            if produto_col not in city_data.columns:
                logger.error(f"DEBUG - Coluna {produto_col} não encontrada para {city}")
                logger.error(f"DEBUG - Colunas disponíveis: {list(city_data.columns)}")
                continue
            
            # Verificar TODOS os combustíveis disponíveis para esta cidade
            combustiveis_disponiveis = city_data[produto_col].unique()
            logger.info(f"DEBUG - Todos combustíveis disponíveis para {city}: {combustiveis_disponiveis}")
            logger.info(f"DEBUG - Contagem por combustível:")
            for produto in combustiveis_disponiveis:
                count = len(city_data[city_data[produto_col] == produto])
                logger.info(f"DEBUG -   {produto}: {count} registros")
            
            # Filtrar por tipo de combustível específico
            fuel_type_normalized = fuel_type.value.upper()
            if fuel_type.value == 'diesel_s10':
                fuel_type_normalized = 'DIESEL_S10'
            
            logger.info(f"DEBUG - Buscando combustível: {fuel_type_normalized}")
            fuel_data = city_data[city_data[produto_col] == fuel_type_normalized]
            
            # Se não encontrou, tentar variações para diesel
            if fuel_data.empty and fuel_type.value in ['diesel', 'diesel_s10']:
                logger.info(f"DEBUG - Tentando variações para diesel...")
                if fuel_type.value == 'diesel':
                    fuel_data = city_data[city_data[produto_col] == 'DIESEL']
                elif fuel_type.value == 'diesel_s10':
                    fuel_data = city_data[city_data[produto_col] == 'DIESEL_S10']
            
            logger.info(f"DEBUG - {city}: {len(fuel_data)} registros de {fuel_type_normalized}")
            
            if fuel_data.empty:
                logger.warning(f"Nenhum dado de {fuel_type.value} para {city}")
                # Verificar se temos algum dado de qualquer combustível
                logger.warning(f"Combustíveis disponíveis: {combustiveis_disponiveis}")
                continue
            
            # Calcular estatísticas
            try:
                avg_price = float(fuel_data[preco_col].mean())
                min_price = float(fuel_data[preco_col].min())
                max_price = float(fuel_data[preco_col].max())
                
                # Garantir que temos coluna de postos
                if postos_col in fuel_data.columns:
                    total_stations = int(fuel_data[postos_col].sum())
                else:
                    total_stations = len(fuel_data)
                
                # Calcular desvio padrão
                if len(fuel_data) > 1:
                    price_std = float(fuel_data[preco_col].std())
                else:
                    price_std = 0.0
                
                logger.info(f"DEBUG - {city}: preço médio: {avg_price}, estações: {total_stations}, registros: {len(fuel_data)}")
                
                # Obter estado e região
                estado = ""
                regiao = ""
                if not fuel_data.empty:
                    if estado_col in fuel_data.columns and not fuel_data[estado_col].empty:
                        estado = str(fuel_data.iloc[0][estado_col])
                    if regiao_col in fuel_data.columns and not fuel_data[regiao_col].empty:
                        regiao = str(fuel_data.iloc[0][regiao_col])
                
                # Criar objeto de combustível
                fuels_data = {
                    fuel_type.value: {
                        'avg': round(avg_price, 3),
                        'min': round(min_price, 3),
                        'max': round(max_price, 3),
                        'stations': total_stations,
                        'std': round(price_std, 3)
                    }
                }
                
                # Criar objeto de comparação
                comparison = CityComparison(
                    city=city_upper,
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
                logger.error(f"Tipo de erro: {type(calc_error)}")
                continue
        
        if len(comparisons) < 2:
            logger.error(f"Comparisons encontradas: {len(comparisons)}")
            logger.error(f"Cidades buscadas: {city_list}")
            logger.error(f"Cidades encontradas: {found_cities}")
            
            # Debug adicional: mostrar o que encontramos para cada cidade
            for city in found_cities:
                logger.error(f"DEBUG FINAL - Cidade {city}:")
                # Verificar dados novamente para debug
                city_data = df[df[municipio_col].astype(str).str.upper() == city]
                if not city_data.empty:
                    logger.error(f"DEBUG FINAL - Total registros: {len(city_data)}")
                    if produto_col in city_data.columns:
                        produtos = city_data[produto_col].unique()
                        logger.error(f"DEBUG FINAL - Produtos: {produtos}")
                        for produto in produtos:
                            count = len(city_data[city_data[produto_col] == produto])
                            logger.error(f"DEBUG FINAL -   {produto}: {count}")
            
            raise HTTPException(
                status_code=404,
                detail=f"Dados insuficientes para comparação. Encontradas {len(comparisons)} cidades com dados para {fuel_type.value}."
            )
        
        # Determinar recomendação
        comparisons = _add_recommendations(comparisons, fuel_type.value)
        
        return comparisons
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /cities: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")

@router.get("/debug/raw-city")
async def debug_raw_city(
    city: str = Query(..., description="Nome da cidade"),
    fuel_type: str = Query("gasolina", description="Tipo de combustível")
):
    """Debug: Mostra dados brutos para uma cidade"""
    try:
        processor = get_processor()
        df = processor.df
        
        city_upper = city.upper()
        
        # Buscar todos os dados da cidade
        city_data = df[df['MUNICIPIO'].astype(str).str.upper() == city_upper]
        
        if city_data.empty:
            return {"found": False, "city": city_upper}
        
        # Mostrar todas as colunas disponíveis
        columns_info = {}
        for col in ['PRODUTO', 'PRODUTO_CONSOLIDADO', 'MUNICIPIO', 'PRECO_MEDIO_REVENDA', 'DATA_INICIAL']:
            if col in city_data.columns:
                columns_info[col] = {
                    "unique_values": city_data[col].unique().tolist(),
                    "sample": city_data[col].head(5).tolist()
                }
        
        # Filtrar por gasolina
        gasolina_data = city_data[
            (city_data['PRODUTO'].astype(str).str.contains('GASOLINA', case=False, na=False)) |
            (city_data['PRODUTO_CONSOLIDADO'].astype(str).str.contains('GASOLINA', case=False, na=False))
        ]
        
        return {
            "found": True,
            "city": city_upper,
            "total_records": len(city_data),
            "gasolina_records": len(gasolina_data),
            "columns_info": columns_info,
            "gasolina_details": gasolina_data[['PRODUTO', 'PRODUTO_CONSOLIDADO', 'PRECO_MEDIO_REVENDA', 'DATA_INICIAL', 'NUMERO_DE_POSTOS_PESQUISADOS']].to_dict('records') if len(gasolina_data) > 0 else []
        }
        
    except Exception as e:
        logger.error(f"Erro em /debug/raw-city: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



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
