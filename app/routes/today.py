from fastapi import APIRouter, Query, HTTPException, Depends
from typing import Optional, List
import logging
from datetime import datetime
from app.services.anp_downloader import ANPDownloader
from app.services.data_processor import DataProcessor
from app.utils.column_helper import get_column_mapping, normalize_city_name
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


@router.get("/debug-raw")
async def debug_raw_data():
    """Endpoint para debug dos dados brutos"""
    try:
        _downloader = ANPDownloader()
        df = _downloader.load_data()
        
        return {
            "total_records": len(df),
            "columns": list(df.columns),
            "column_types": df.dtypes.astype(str).to_dict(),
            "unique_products": df['PRODUTO'].unique().tolist() if 'PRODUTO' in df.columns else [],
            "sample_products": df['PRODUTO'].head(20).tolist() if 'PRODUTO' in df.columns else [],
            "has_diesel": 'OLEO DIESEL' in df['PRODUTO'].values if 'PRODUTO' in df.columns else False,
            "has_diesel_s10": 'OLEO DIESEL S10' in df['PRODUTO'].values if 'PRODUTO' in df.columns else False,
            "sample_data": df[['PRODUTO', 'MUNICIPIO', 'ESTADO', 'PRECO_MEDIO_REVENDA']].head(10).to_dict('records')
        }
    except Exception as e:
        logger.error(f"Erro em /debug-raw: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/best-price", response_model=BestPriceResponse)
async def get_best_price(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Retorna o melhor preço atual por tipo de combustível usando dados da última semana"""
    try:
        processor = get_processor()
        
        # **CORREÇÃO: Usar apenas dados da última semana**
        latest_data = processor.get_latest_week_data()
        
        if latest_data.empty:
            raise HTTPException(
                status_code=404,
                detail="Nenhum dado da última semana disponível"
            )
        
        # Filtrar por tipo de combustível
        product_column = 'PRODUTO_CONSOLIDADO' if 'PRODUTO_CONSOLIDADO' in latest_data.columns else 'PRODUTO'
        fuel_df = latest_data[latest_data[product_column] == fuel_type.value.upper()]
        
        if fuel_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value} na última semana"
            )
        
        # Encontrar melhor preço
        price_column = 'PRECO_MEDIO_REVENDA' if 'PRECO_MEDIO_REVENDA' in fuel_df.columns else 'PRECO MEDIO REVENDA'
        if price_column not in fuel_df.columns:
            # Tentar encontrar qualquer coluna com preço
            for col in fuel_df.columns:
                if 'preco' in col.lower() or 'price' in col.lower():
                    price_column = col
                    break
        
        best_idx = fuel_df[price_column].idxmin()
        best_row = fuel_df.loc[best_idx]
        
        # Construir resposta
        result = {
            'price': float(best_row[price_column]),
            'city': str(best_row.get('MUNICIPIO', '')),
            'state': str(best_row.get('ESTADO_SIGLA', best_row.get('ESTADO', ''))),
            'region': str(best_row.get('REGIAO', '')),
            'fuel_type': fuel_type.value,
            'stations_count': int(best_row.get('NUMERO_DE_POSTOS_PESQUISADOS', 0)),
            'price_band': best_row.get('FAIXA_PRECO', 'MEDIO') if 'FAIXA_PRECO' in best_row else 'MEDIO'
        }
        
        # Adicionar coordenadas se disponíveis
        if 'LATITUDE' in best_row and 'LONGITUDE' in best_row:
            result['latitude'] = float(best_row['LATITUDE'])
            result['longitude'] = float(best_row['LONGITUDE'])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /best-price: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/debug/latest-dates")
async def debug_latest_dates():
    """Debug: Verificar datas dos dados"""
    try:
        processor = get_processor()
        df = processor.df
        
        # Verificar todas as datas disponíveis
        if 'DATA_FINAL' in df.columns:
            unique_dates = df['DATA_FINAL'].unique()
            sorted_dates = sorted(unique_dates, reverse=True)
            return {
                "total_records": len(df),
                "unique_dates_count": len(unique_dates),
                "latest_5_dates": [str(date) for date in sorted_dates[:5]],
                "latest_date": str(df['DATA_FINAL'].max()),
                "oldest_date": str(df['DATA_FINAL'].min()),
                "sample_data_latest": df[df['DATA_FINAL'] == df['DATA_FINAL'].max()][['MUNICIPIO', 'PRODUTO', 'PRECO_MEDIO_REVENDA', 'DATA_FINAL']].head(5).to_dict('records')
            }
        
        return {"error": "Coluna DATA_FINAL não encontrada", "columns": list(df.columns)}
        
    except Exception as e:
        logger.error(f"Erro em /debug/latest-dates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
    """Retorna resumo completo para a página 'Onde abastecer hoje?' usando apenas dados da última semana"""
    try:
        processor = get_processor()
        
        # **CORREÇÃO: Usar apenas dados da última semana**
        latest_data = processor.get_latest_week_data()
        
        if latest_data.empty:
            raise HTTPException(
                status_code=404,
                detail="Nenhum dado da última semana disponível"
            )
        
        # **CORREÇÃO: Obter data dos dados mais recentes**
        latest_date = processor.get_latest_data_timestamp()
        
        # DEBUG detalhado
        logger.info(f"DEBUG - Dados da última semana: {len(latest_data)} registros")
        logger.info(f"DEBUG - Data dos dados: {latest_date}")
        logger.info(f"DEBUG - Colunas disponíveis: {list(latest_data.columns)}")
        
        # Verificar qual coluna de produto usar
        product_column = None
        possible_product_columns = ['PRODUTO_CONSOLIDADO', 'produto_consolidado', 'PRODUTO', 'produto']
        
        for col in possible_product_columns:
            if col in latest_data.columns:
                product_column = col
                logger.info(f"DEBUG - Usando coluna de produto: {product_column}")
                logger.info(f"DEBUG - Valores únicos: {latest_data[product_column].unique()[:5]}")
                break
        
        if product_column is None:
            logger.error(f"DEBUG - Coluna de produto não encontrada. Colunas: {list(latest_data.columns)}")
            raise HTTPException(
                status_code=500,
                detail="Erro: coluna de produto não encontrada"
            )
        
        # Filtrar por tipo de combustível
        fuel_df = latest_data[latest_data[product_column] == fuel_type.value.upper()]
        
        if fuel_df.empty:
            logger.error(f"DEBUG - Nenhum dado para {fuel_type.value.upper()} na última semana")
            logger.error(f"DEBUG - Valores disponíveis em {product_column}: {latest_data[product_column].unique()[:10]}")
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value} na última semana"
            )
        
        # Encontrar coluna de preço
        price_column = None
        possible_price_columns = ['PRECO_MEDIO_REVENDA', 'preco_medio_revenda', 'PRECO MEDIO REVENDA', 'preco medio revenda']
        
        for col in possible_price_columns:
            if col in fuel_df.columns:
                price_column = col
                logger.info(f"DEBUG - Usando coluna de preço: {price_column}")
                break
        
        if price_column is None:
            logger.error(f"DEBUG - Coluna de preço não encontrada. Colunas: {list(fuel_df.columns)}")
            raise HTTPException(
                status_code=500,
                detail="Erro na estrutura dos dados: coluna de preço não encontrada"
            )
        
        # Encontrar coluna de postos
        stations_column = None
        possible_stations_columns = ['NUMERO_DE_POSTOS_PESQUISADOS', 'numero_de_postos_pesquisados', 
                                     'NUMERO DE POSTOS PESQUISADOS', 'numero de postos pesquisados']
        
        for col in possible_stations_columns:
            if col in fuel_df.columns:
                stations_column = col
                logger.info(f"DEBUG - Usando coluna de postos: {stations_column}")
                break
        
        if stations_column is None:
            stations_column = 'NUMERO_DE_POSTOS_PESQUISADOS'
            logger.warning(f"DEBUG - Coluna de postos não encontrada, usando padrão")
        
        # **CORREÇÃO: Encontrar melhor e pior preço na última semana**
        if fuel_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value} na última semana"
            )
        
        # Melhor preço (menor)
        best_idx = fuel_df[price_column].idxmin()
        best_row = fuel_df.loc[best_idx]
        
        best_price = {
            'price': float(best_row[price_column]),
            'city': str(best_row.get('MUNICIPIO', best_row.get('municipio', ''))),
            'state': str(best_row.get('ESTADO', best_row.get('estado', ''))),
            'region': str(best_row.get('REGIAO', best_row.get('regiao', ''))),
            'stations_count': int(best_row.get(stations_column, 0)),
            'fuel_type': fuel_type.value,
            'price_band': best_row.get('FAIXA_PRECO', 'MEDIO') if 'FAIXA_PRECO' in best_row else 'MEDIO'
        }
        
        # Pior preço (maior)
        worst_idx = fuel_df[price_column].idxmax()
        worst_row = fuel_df.loc[worst_idx]
        
        worst_price = {
            'price': float(worst_row[price_column]),
            'city': str(worst_row.get('MUNICIPIO', worst_row.get('municipio', ''))),
            'state': str(worst_row.get('ESTADO', worst_row.get('estado', ''))),
            'region': str(worst_row.get('REGIAO', worst_row.get('regiao', ''))),
            'stations_count': int(worst_row.get(stations_column, 0))
        }
        
        # Economia potencial
        potential_saving = worst_price['price'] - best_price['price']
        
        # **CORREÇÃO: Total de postos da última semana (sem duplicação por cidade-produto)**
        # Evitar somar postos múltiplas vezes para mesma cidade e produto
        if 'MUNICIPIO' in fuel_df.columns and 'PRODUTO_CONSOLIDADO' in fuel_df.columns:
            # Agrupar por cidade e produto, pegar o máximo de postos (evita duplicação)
            grouped_stations = fuel_df.groupby(['MUNICIPIO', 'PRODUTO_CONSOLIDADO'])[stations_column].max().reset_index()
            total_stations = int(grouped_stations[stations_column].sum())
            logger.info(f"DEBUG - Postos após agrupamento: {total_stations}")
        else:
            total_stations = int(fuel_df[stations_column].sum() if stations_column in fuel_df.columns else 0)
            logger.info(f"DEBUG - Postos sem agrupamento: {total_stations}")
        
        # Média nacional da última semana
        national_average = float(fuel_df[price_column].mean())
        
        # **CORREÇÃO: Ranking usando apenas dados da última semana**
        # Para o ranking, precisamos adaptar ou usar o ranking normal
        # Por enquanto, usamos o método existente que ainda usa todos os dados
        # Mais tarde podemos criar um método específico para última semana
        ranking = processor.get_ranking(fuel_type.value, 10)
        
        return SummaryResponse(
            best_price=best_price,
            worst_price=worst_price,
            potential_saving=round(potential_saving, 3),
            total_stations=total_stations,
            # **CORREÇÃO: Usar data dos dados, não data do processamento**
            analysis_date=latest_date if latest_date else datetime.now(),
            ranking=ranking,
            national_average=round(national_average, 3),
            # Adicionar data dos dados para referência
            data_date=latest_date
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/debug/city-data")
async def debug_city_data(
    city: str = Query(..., description="Nome da cidade"),
    fuel_type: str = Query("gasolina", description="Tipo de combustível")
):
    """Debug: Verifica se há dados para uma cidade específica"""
    try:
        processor = get_processor()
        df = processor.df
        
        logger.info(f"DEBUG - Buscando {city.upper()} para {fuel_type.upper()}")
        logger.info(f"DEBUG - Colunas disponíveis: {list(df.columns)}")
        
        # Normalizar nome da cidade
        from app.utils.column_helper import normalize_city_name
        city_normalized = normalize_city_name(city)
        logger.info(f"DEBUG - Cidade normalizada: {city_normalized}")
        
        # Buscar cidade
        city_mask = df['MUNICIPIO'].str.upper() == city_normalized
        city_data = df[city_mask]
        
        if city_data.empty:
            # Tentar busca por contém
            city_mask = df['MUNICIPIO'].str.contains(city.upper(), na=False)
            city_data = df[city_mask]
            
            if city_data.empty:
                # Tentar sem acentos
                city_mask = df['MUNICIPIO'].astype(str).apply(normalize_city_name) == city_normalized
                city_data = df[city_mask]
        
        logger.info(f"DEBUG - Registros encontrados: {len(city_data)}")
        
        if not city_data.empty:
            # Verificar produtos disponíveis
            produtos = city_data['PRODUTO_CONSOLIDADO'].unique()
            logger.info(f"DEBUG - Produtos disponíveis: {produtos}")
            
            # Filtrar por combustível específico
            fuel_type_normalized = fuel_type.upper()
            if fuel_type == 'diesel_s10':
                fuel_type_normalized = 'DIESEL_S10'
            
            fuel_data = city_data[city_data['PRODUTO_CONSOLIDADO'] == fuel_type_normalized]
            logger.info(f"DEBUG - Registros de {fuel_type}: {len(fuel_data)}")
            
            if not fuel_data.empty:
                return {
                    "found": True,
                    "city": city_normalized,
                    "fuel_type": fuel_type,
                    "records_count": len(fuel_data),
                    "avg_price": float(fuel_data['PRECO_MEDIO_REVENDA'].mean()),
                    "min_price": float(fuel_data['PRECO_MEDIO_REVENDA'].min()),
                    "max_price": float(fuel_data['PRECO_MEDIO_REVENDA'].max()),
                    "total_stations": int(fuel_data['NUMERO_DE_POSTOS_PESQUISADOS'].sum()),
                    "sample_data": fuel_data[['PRODUTO', 'PRODUTO_CONSOLIDADO', 'PRECO_MEDIO_REVENDA', 'NUMERO_DE_POSTOS_PESQUISADOS']].head(5).to_dict('records')
                }
        
        return {
            "found": False,
            "city": city_normalized,
            "fuel_type": fuel_type,
            "available_fuels": list(city_data['PRODUTO_CONSOLIDADO'].unique()) if not city_data.empty else [],
            "all_cities_sample": df['MUNICIPIO'].unique()[:20].tolist() if 'MUNICIPIO' in df.columns else []
        }
        
    except Exception as e:
        logger.error(f"Erro em /debug/city-data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_general_stats():
    """Retorna estatísticas gerais do sistema usando dados da última semana"""
    try:
        processor = get_processor()
        
        # **CORREÇÃO: Usar apenas dados da última semana**
        df = processor.get_latest_week_data()
        
        # Usar helper para mapeamento correto
        col_map = get_column_mapping(df)
        
        # Garantir que temos dados válidos
        if df.empty:
            raise HTTPException(
                status_code=503,
                detail="Nenhum dado da última semana disponível"
            )
        
        # **CORREÇÃO: Obter data dos dados mais recentes**
        latest_date = processor.get_latest_data_timestamp()
        
        # Calcular postos corretamente (evitar duplicação)
        postos_col = col_map.get('numero_de_postos_pesquisados', 'NUMERO_DE_POSTOS_PESQUISADOS')
        municipio_col = col_map.get('municipio', 'MUNICIPIO')
        produto_col = col_map.get('produto_consolidado', 'PRODUTO_CONSOLIDADO')
        
        total_stations = 0
        if postos_col in df.columns and municipio_col in df.columns and produto_col in df.columns:
            # Agrupar para evitar duplicação
            grouped = df.groupby([municipio_col, produto_col])[postos_col].max().reset_index()
            total_stations = int(grouped[postos_col].sum())
        else:
            total_stations = int(df[postos_col].sum() if postos_col in df.columns else 0)
        
        stats = {
            "total_records": len(df),
            "total_municipalities": df[municipio_col].nunique() if municipio_col in df.columns else 0,
            "total_states": df[col_map.get('estado', 'ESTADO')].nunique() if 'estado' in col_map else 0,
            "total_fuel_types": df[produto_col].nunique() if produto_col in df.columns else 0,
            "data_coverage": {
                "norte": len(df[df[col_map.get('regiao', 'REGIAO')].astype(str).str.contains('NORTE', case=False, na=False)]) if 'regiao' in col_map else 0,
                "nordeste": len(df[df[col_map.get('regiao', 'REGIAO')].astype(str).str.contains('NORDESTE', case=False, na=False)]) if 'regiao' in col_map else 0,
                "centro_oeste": len(df[df[col_map.get('regiao', 'REGIAO')].astype(str).str.contains('CENTRO.OESTE', case=False, na=False)]) if 'regiao' in col_map else 0,
                "sudeste": len(df[df[col_map.get('regiao', 'REGIAO')].astype(str).str.contains('SUDESTE', case=False, na=False)]) if 'regiao' in col_map else 0,
                "sul": len(df[df[col_map.get('regiao', 'REGIAO')].astype(str).str.contains('SUL', case=False, na=False)]) if 'regiao' in col_map else 0
            },
            "stations_analyzed": total_stations,
            # **CORREÇÃO: Usar data dos dados, não data do cache**
            "last_update": latest_date.isoformat() if latest_date else None,
            "data_date": latest_date.isoformat() if latest_date else None,
            "cache_status": "fresh" if not cache.should_refresh() else "stale"
        }
        
        # Adicionar preços se disponíveis
        preco_col = col_map.get('preco_medio_revenda', 'PRECO_MEDIO_REVENDA')
        if preco_col in df.columns:
            stats["price_range"] = {
                "min": float(df[preco_col].min()),
                "max": float(df[preco_col].max()),
                "average": float(df[preco_col].mean()),
                "median": float(df[preco_col].median())
            }
        
        return stats
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search")
async def search_cities(
    query: str = Query(..., min_length=2, description="Termo de busca"),
    limit: int = Query(10, ge=1, le=50, description="Número de resultados")
):
    """Busca municípios por nome"""
    try:
        processor = get_processor()
        df = processor.df
        
        # Usar helper para mapeamento correto
        col_map = get_column_mapping(df)
        
        # Normalizar query
        query_normalized = normalize_city_name(query)
        
        # Filtrar municípios que contenham o termo
        results = df[
            df[col_map['municipio']].astype(str).apply(normalize_city_name).str.contains(query_normalized)
        ]
        
        if results.empty:
            return []
        
        # Agrupar por município
        grouped = results.groupby([
            col_map['municipio'], 
            col_map['estado'], 
            col_map['regiao']
        ]).agg({
            col_map['preco_medio_revenda']: 'mean',
            col_map['numero_de_postos_pesquisados']: 'sum',
            col_map['produto_consolidado']: lambda x: list(x.unique())
        }).reset_index()
        
        # Limitar resultados
        grouped = grouped.head(limit)
        
        return [
            {
                'city': row[col_map['municipio']],
                'state': row[col_map['estado']],
                'region': row[col_map['regiao']],
                'avg_price': float(row[col_map['preco_medio_revenda']]),
                'stations': int(row[col_map['numero_de_postos_pesquisados']]),
                'available_fuels': row[col_map['produto_consolidado']]
            }
            for _, row in grouped.iterrows()
        ]
        
    except Exception as e:
        logger.error(f"Erro em /search: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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
