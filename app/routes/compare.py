from fastapi import APIRouter, Query, HTTPException
from typing import List
import logging
from datetime import datetime
from app.services.anp_downloader import ANPDownloader
from app.services.data_processor import DataProcessor
from app.services.cache_manager import cache
from app.models.schemas import CityComparison, FuelType

router = APIRouter()
logger = logging.getLogger(__name__)

def get_processor():
    """Obter processador de dados"""
    from app.routes.today import get_processor as get_global_processor
    return get_global_processor()

@router.get("/cities", response_model=List[CityComparison])
async def compare_cities(
    cities: str = Query(..., description="Lista de cidades separadas por vírgula"),
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Compara preços entre múltiplas cidades"""
    try:
        # Parse da lista de cidades
        city_list = [city.strip().upper() for city in cities.split(',')]
        
        if len(city_list) < 2:
            raise HTTPException(
                status_code=400,
                detail="Forneça pelo menos duas cidades para comparação"
            )
        
        processor = get_processor()
        comparisons = []
        
        for city in city_list:
            # Filtrar dados da cidade
            city_data = processor.df[
                (processor.df['municipio'] == city) & 
                (processor.df['produto_consolidado'] == fuel_type.value.upper())
            ]
            
            if city_data.empty:
                logger.warning(f"Cidade não encontrada: {city}")
                continue
            
            # Calcular estatísticas
            fuels_data = {
                fuel_type.value: {
                    'avg': float(city_data['preco_medio_revenda'].mean()),
                    'min': float(city_data['preco_medio_revenda'].min()),
                    'max': float(city_data['preco_medio_revenda'].max()),
                    'stations': int(city_data['numero_de_postos_pesquisados'].sum())
                }
            }
            
            comparisons.append(CityComparison(
                city=city,
                state=city_data.iloc[0]['estado'],
                region=city_data.iloc[0]['regiao'],
                fuels=fuels_data,
                overall_stats={
                    'total_stations': int(city_data['numero_de_postos_pesquisados'].sum()),
                    'city_count': 1
                }
            ))
        
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
        logger.error(f"Erro em /cities: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

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
