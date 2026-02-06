from fastapi import APIRouter, Query, HTTPException, Depends
from typing import Optional, List
import logging
from datetime import datetime
from app.services.anp_downloader import ANPDownloader
from app.services.data_processor import DataProcessor
from app.services.cache_manager import cache
from app.models.schemas import (
    BestPriceResponse, RankingItem, RegionStats, 
    SummaryResponse, FuelType, Region
)

router = APIRouter()
logger = logging.getLogger(__name__)

# Instância única
_downloader = None
_processor = None

def get_processor():
    """Obter processador de dados (com cache)"""
    global _downloader, _processor
    
    if _processor is None or cache.should_refresh():
        try:
            logger.info("Carregando dados da ANP...")
            _downloader = ANPDownloader()
            df = _downloader.load_data()
            _processor = DataProcessor(df)
            cache.update_timestamp()
            logger.info(f"Dados carregados: {len(df)} registros")
        except Exception as e:
            logger.error(f"Erro ao carregar processador: {e}")
            if _processor is None:
                raise HTTPException(
                    status_code=503,
                    detail="Serviço de dados indisponível. Tente novamente em alguns minutos."
                )
            # Se já temos dados antigos, continuamos com eles
            logger.warning("Continuando com dados em cache")
    
    return _processor

@router.get("/best-price", response_model=BestPriceResponse)
async def get_best_price(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Retorna o melhor preço atual por tipo de combustível"""
    try:
        processor = get_processor()
        result = processor.get_best_price_by_fuel(fuel_type.value)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value}"
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /best-price: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/ranking", response_model=List[RankingItem])
async def get_ranking(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível"),
    limit: int = Query(10, ge=1, le=50, description="Número de resultados (1-50)")
):
    """Retorna ranking dos municípios mais baratos para um tipo de combustível"""
    try:
        processor = get_processor()
        ranking = processor.get_ranking(fuel_type.value, limit)
        
        if not ranking:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value}"
            )
        
        return ranking
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /ranking: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/regions", response_model=List[RegionStats])
async def get_regions_data(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Retorna dados agregados por região para colorir o mapa"""
    try:
        processor = get_processor()
        stats = processor.get_region_stats()
        
        # Filtrar por tipo de combustível
        fuel_stats = [s for s in stats if s['fuel_type'] == fuel_type.value]
        
        if not fuel_stats:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value}"
            )
        
        # Calcular escala de cores (0-100)
        prices = [s['avg_price'] for s in fuel_stats]
        min_price = min(prices) if prices else 0
        max_price = max(prices) if prices else 1
        
        # Adicionar índice de cor
        for stat in fuel_stats:
            normalized = ((stat['avg_price'] - min_price) / (max_price - min_price)) * 100
            stat['color_index'] = float(normalized)
        
        return fuel_stats
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /regions: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/summary", response_model=SummaryResponse)
async def get_today_summary(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Retorna resumo completo para a página 'Onde abastecer hoje?'"""
    try:
        processor = get_processor()
        
        # Melhor preço
        best_price = processor.get_best_price_by_fuel(fuel_type.value)
        
        if not best_price:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value}"
            )
        
        # Pior preço - CORREÇÃO DO ERRO 'preco_medio_revenda'
        # DEBUG: Verificar colunas disponíveis
        logger.info(f"DEBUG - Colunas disponíveis: {list(processor.df.columns)}")
        
        # O data_processor converte para minúsculas, então usamos minúsculas
        fuel_df = processor.df[processor.df['produto_consolidado'] == fuel_type.value.upper()]
        
        if fuel_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value}"
            )
        
        # VERIFICAÇÃO CRÍTICA: Encontrar o nome correto da coluna
        price_column = None
        possible_names = ['preco_medio_revenda', 'preco medio revenda', 
                         'PRECO_MEDIO_REVENDA', 'PRECO MEDIO REVENDA']
        
        for name in possible_names:
            if name in fuel_df.columns:
                price_column = name
                logger.info(f"DEBUG - Usando coluna de preço: {price_column}")
                break
        
        if price_column is None:
            logger.error(f"DEBUG - Coluna de preço não encontrada. Colunas: {list(fuel_df.columns)}")
            raise HTTPException(
                status_code=500,
                detail="Erro na estrutura dos dados: coluna de preço não encontrada"
            )
        
        # Agora usar a coluna correta
        worst_idx = fuel_df[price_column].idxmax()
        worst_row = fuel_df.loc[worst_idx]
        
        # Acessar outras colunas (também em minúsculas)
        stations_column = 'numero_de_postos_pesquisados'
        if stations_column not in fuel_df.columns:
            stations_column = 'numero de postos pesquisados'  # Tentar alternativa
        
        worst_price = {
            'price': float(worst_row[price_column]),
            'city': worst_row['municipio'],
            'state': worst_row['estado'],
            'region': worst_row['regiao'],
            'stations_count': int(worst_row.get(stations_column, 0))
        }
        
        # Economia potencial
        potential_saving = worst_price['price'] - best_price['price']
        
        # Total de postos
        total_stations = int(processor.df[stations_column].sum())
        
        # Média nacional 
        national_average = float(fuel_df[price_column].mean())
        
        # Ranking
        ranking = processor.get_ranking(fuel_type.value, 10)
        
        return SummaryResponse(
            best_price=best_price,
            worst_price=worst_price,
            potential_saving=round(potential_saving, 3),
            total_stations=total_stations,
            analysis_date=datetime.now(),
            ranking=ranking,
            national_average=round(national_average, 3)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /summary: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/stats")
async def get_general_stats():
    """Retorna estatísticas gerais do sistema"""
    try:
        processor = get_processor()
        df = processor.df
        
        stats = {
            "total_records": len(df),
            "total_municipalities": df['municipio'].nunique(),
            "total_states": df['estado'].nunique(),
            "total_fuel_types": df['produto_consolidado'].nunique(),  # MINÚSCULO
            "data_coverage": {
                "norte": len(df[df['regiao'] == 'NORTE']),
                "nordeste": len(df[df['regiao'] == 'NORDESTE']),
                "centro_oeste": len(df[df['regiao'] == 'CENTRO_OESTE']),
                "sudeste": len(df[df['regiao'] == 'SUDESTE']),
                "sul": len(df[df['regiao'] == 'SUL'])
            },
            "price_range": {
                "min": float(df['preco_medio_revenda'].min()),
                "max": float(df['preco_medio_revenda'].max()),
                "average": float(df['preco_medio_revenda'].mean()),
                "median": float(df['preco_medio_revenda'].median())
            },
            "stations_analyzed": int(df['numero_de_postos_pesquisados'].sum()),
            "last_update": cache.get_timestamp().isoformat() if cache.get_timestamp() else None,
            "cache_status": "fresh" if not cache.should_refresh() else "stale"
        }
        
        return stats
    except Exception as e:
        logger.error(f"Erro em /stats: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/search")
async def search_cities(
    query: str = Query(..., min_length=2, description="Termo de busca"),
    limit: int = Query(10, ge=1, le=50, description="Número de resultados")
):
    """Busca municípios por nome"""
    try:
        processor = get_processor()
        df = processor.df
        
        # Filtrar municípios que contenham o termo
        results = df[df['municipio'].str.contains(query.upper())]
        
        if results.empty:
            return []
        
        # Agrupar por município
        grouped = results.groupby(['municipio', 'estado', 'regiao']).agg({
            'preco_medio_revenda': 'mean',
            'numero_de_postos_pesquisados': 'sum',
            'produto_consolidado': lambda x: list(x.unique())  # MINÚSCULO
        }).reset_index()
        
        # Limitar resultados
        grouped = grouped.head(limit)
        
        return [
            {
                'city': row['municipio'],
                'state': row['estado'],
                'region': row['regiao'],
                'avg_price': float(row['preco_medio_revenda']),
                'stations': int(row['numero_de_postos_pesquisados']),
                'available_fuels': row['produto_consolidado']
            }
            for _, row in grouped.iterrows()
        ]
        
    except Exception as e:
        logger.error(f"Erro em /search: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")


@router.get("/debug-data")
async def debug_data():
    """Endpoint para debug dos dados"""
    try:
        processor = get_processor()
        df = processor.df
        
        return {
            "total_records": len(df),
            "columns": list(df.columns),
            "sample_records": df.head(5).to_dict('records'),
            "column_types": df.dtypes.astype(str).to_dict(),
            "unique_products": df['produto_consolidado'].unique().tolist() if 'produto_consolidado' in df.columns else [],
            "price_column_exists": 'preco_medio_revenda' in df.columns
        }
    except Exception as e:
        logger.error(f"Erro em /debug-data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
