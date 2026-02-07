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
        
        # Usar apenas dados da última semana
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
        
        # **FILTRO DE CONFIABILIDADE**
        MIN_POSTOS_CONFIAVEL = 5
        MIN_POSTOS_POR_CIDADE = 10
        
        # 1. Filtrar registros com poucos postos
        fuel_df_confiavel = fuel_df[fuel_df['NUMERO_DE_POSTOS_PESQUISADOS'] >= MIN_POSTOS_CONFIAVEL].copy()
        
        if fuel_df_confiavel.empty:
            logger.warning(f"Nenhum registro com pelo menos {MIN_POSTOS_CONFIAVEL} postos. Reduzindo para 3.")
            MIN_POSTOS_CONFIAVEL = 3
            fuel_df_confiavel = fuel_df[fuel_df['NUMERO_DE_POSTOS_PESQUISADOS'] >= MIN_POSTOS_CONFIAVEL].copy()
        
        if fuel_df_confiavel.empty:
            logger.warning("Usando todos os dados")
            fuel_df_confiavel = fuel_df
        
        # 2. Agrupar por cidade
        city_grouped = fuel_df_confiavel.groupby(['MUNICIPIO', 'ESTADO', 'REGIAO']).agg({
            'PRECO_MEDIO_REVENDA': 'mean',
            'NUMERO_DE_POSTOS_PESQUISADOS': 'sum'
        }).reset_index()
        
        # 3. Filtrar cidades com poucos postos totais
        city_grouped_confiavel = city_grouped[city_grouped['NUMERO_DE_POSTOS_PESQUISADOS'] >= MIN_POSTOS_POR_CIDADE].copy()
        
        if city_grouped_confiavel.empty:
            logger.warning(f"Nenhuma cidade com pelo menos {MIN_POSTOS_POR_CIDADE} postos. Reduzindo para 5.")
            MIN_POSTOS_POR_CIDADE = 5
            city_grouped_confiavel = city_grouped[city_grouped['NUMERO_DE_POSTOS_PESQUISADOS'] >= MIN_POSTOS_POR_CIDADE].copy()
        
        if city_grouped_confiavel.empty:
            logger.warning("Usando todas as cidades")
            city_grouped_confiavel = city_grouped
        
        if city_grouped_confiavel.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhuma cidade com dados confiáveis para {fuel_type.value}"
            )
        
        # 4. Encontrar melhor preço
        best_idx = city_grouped_confiavel['PRECO_MEDIO_REVENDA'].idxmin()
        best_row = city_grouped_confiavel.loc[best_idx]
        
        # Construir resposta
        result = {
            'price': float(best_row['PRECO_MEDIO_REVENDA']),
            'city': str(best_row['MUNICIPIO']),
            'state': str(best_row['ESTADO']),
            'region': str(best_row['REGIAO']),
            'fuel_type': fuel_type.value,
            'stations_count': int(best_row['NUMERO_DE_POSTOS_PESQUISADOS']),
            'price_band': 'BAIXO',
            'reliability': 'high' if best_row['NUMERO_DE_POSTOS_PESQUISADOS'] >= 10 else 'medium'
        }
        
        # Adicionar coordenadas
        coords = processor._estimate_coordinates(best_row['MUNICIPIO'], best_row['ESTADO'])
        result['latitude'] = coords['latitude']
        result['longitude'] = coords['longitude']
        
        logger.info(f"Melhor preço encontrado: {result['city']} - R${result['price']:.2f} ({result['stations_count']} postos)")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /best-price: {e}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@router.get("/debug/best-price-investigation")
async def debug_best_price_investigation(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Debug: Investigar porque o melhor preço está errado"""
    try:
        processor = get_processor()
        
        # Usar apenas dados da última semana
        latest_data = processor.get_latest_week_data()
        
        if latest_data.empty:
            return {"error": "Nenhum dado da última semana"}
        
        # Filtrar por gasolina
        fuel_df = latest_data[latest_data['PRODUTO_CONSOLIDADO'] == fuel_type.value.upper()]
        
        if fuel_df.empty:
            return {"error": f"Nenhum dado para {fuel_type.value}"}
        
        # **ANÁLISE DETALHADA:**
        
        # 1. Todas as ocorrências de Aguas Lindas de Goias
        aguas_lindas = fuel_df[fuel_df['MUNICIPIO'].str.contains('AGUAS LINDAS', case=False, na=False)]
        
        # 2. Top 10 menores preços médios
        top_10_avg = fuel_df.nsmallest(10, 'PRECO_MEDIO_REVENDA')[['MUNICIPIO', 'ESTADO', 'PRECO_MEDIO_REVENDA', 'PRECO_MINIMO_REVENDA', 'NUMERO_DE_POSTOS_PESQUISADOS', 'PRODUTO']]
        
        # 3. Top 10 menores preços mínimos  
        top_10_min = fuel_df.nsmallest(10, 'PRECO_MINIMO_REVENDA')[['MUNICIPIO', 'ESTADO', 'PRECO_MEDIO_REVENDA', 'PRECO_MINIMO_REVENDA', 'NUMERO_DE_POSTOS_PESQUISADOS', 'PRODUTO']]
        
        # 4. Agrupamento por cidade para ver média
        city_grouped = fuel_df.groupby(['MUNICIPIO', 'ESTADO']).agg({
            'PRECO_MEDIO_REVENDA': 'mean',
            'PRECO_MINIMO_REVENDA': 'min',
            'NUMERO_DE_POSTOS_PESQUISADOS': 'sum'
        }).reset_index()
        
        # Top 10 cidades com menor preço médio (agrupado)
        top_10_cities_avg = city_grouped.nsmallest(10, 'PRECO_MEDIO_REVENDA')
        
        # Top 10 cidades com menor preço mínimo (agrupado)
        top_10_cities_min = city_grouped.nsmallest(10, 'PRECO_MINIMO_REVENDA')
        
        return {
            "investigation_for": fuel_type.value,
            "latest_week_date": str(processor.get_latest_data_timestamp()),
            "total_records_last_week": len(fuel_df),
            
            "aguas_lindas_details": aguas_lindas[['PRODUTO', 'PRECO_MEDIO_REVENDA', 'PRECO_MINIMO_REVENDA', 'NUMERO_DE_POSTOS_PESQUISADOS']].to_dict('records') if len(aguas_lindas) > 0 else "Não encontrado",
            
            "top_10_lowest_avg_prices": top_10_avg.to_dict('records'),
            "top_10_lowest_min_prices": top_10_min.to_dict('records'),
            
            "top_10_cities_lowest_avg": top_10_cities_avg.to_dict('records'),
            "top_10_cities_lowest_min": top_10_cities_min.to_dict('records'),
            
            "potential_issues": [
                "Verificar se está usando dados corretos (última semana)",
                "Verificar agrupamento por cidade (GASOLINA COMUM vs ADITIVADA)",
                "Verificar filtro de número mínimo de postos"
            ]
        }
        
    except Exception as e:
        logger.error(f"Erro em /debug/best-price-investigation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
        
        # Usar apenas dados da última semana
        latest_data = processor.get_latest_week_data()
        
        if latest_data.empty:
            raise HTTPException(
                status_code=404,
                detail="Nenhum dado da última semana disponível"
            )
        
        # Obter data dos dados mais recentes
        latest_date = processor.get_latest_data_timestamp()
        
        logger.info(f"DEBUG - Dados da última semana: {len(latest_data)} registros")
        logger.info(f"DEBUG - Data dos dados: {latest_date}")
        
        # Filtrar por tipo de combustível
        product_column = 'PRODUTO_CONSOLIDADO' if 'PRODUTO_CONSOLIDADO' in latest_data.columns else 'PRODUTO'
        fuel_df = latest_data[latest_data[product_column] == fuel_type.value.upper()]
        
        if fuel_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum dado encontrado para {fuel_type.value} na última semana"
            )
        
        logger.info(f"DEBUG - Total registros de {fuel_type.value}: {len(fuel_df)}")
        
        # **CONFIGURAÇÃO DE CONFIABILIDADE**
        MIN_POSTOS_CONFIAVEL = 5  # Mínimo de postos por registro
        MIN_POSTOS_POR_CIDADE = 10  # Mínimo de postos totais por cidade
        
        # 1. Filtrar registros com poucos postos
        fuel_df_confiavel = fuel_df[fuel_df['NUMERO_DE_POSTOS_PESQUISADOS'] >= MIN_POSTOS_CONFIAVEL].copy()
        
        if fuel_df_confiavel.empty:
            logger.warning(f"Nenhum registro com pelo menos {MIN_POSTOS_CONFIAVEL} postos. Reduzindo para 3.")
            MIN_POSTOS_CONFIAVEL = 3
            fuel_df_confiavel = fuel_df[fuel_df['NUMERO_DE_POSTOS_PESQUISADOS'] >= MIN_POSTOS_CONFIAVEL].copy()
        
        if fuel_df_confiavel.empty:
            logger.warning("Usando todos os dados (sem filtro de postos)")
            fuel_df_confiavel = fuel_df.copy()
        
        logger.info(f"Dados confiáveis (≥{MIN_POSTOS_CONFIAVEL} postos): {len(fuel_df_confiavel)} registros")
        
        # 2. Agrupar por cidade
        city_grouped = fuel_df_confiavel.groupby(['MUNICIPIO', 'ESTADO', 'REGIAO']).agg({
            'PRECO_MEDIO_REVENDA': 'mean',  # Média entre GASOLINA COMUM e ADITIVADA
            'PRECO_MINIMO_REVENDA': 'min',   # Menor preço mínimo
            'NUMERO_DE_POSTOS_PESQUISADOS': 'sum'
        }).reset_index()
        
        # 3. Filtrar cidades com poucos postos totais
        city_grouped_confiavel = city_grouped[city_grouped['NUMERO_DE_POSTOS_PESQUISADOS'] >= MIN_POSTOS_POR_CIDADE].copy()
        
        if city_grouped_confiavel.empty:
            logger.warning(f"Nenhuma cidade com pelo menos {MIN_POSTOS_POR_CIDADE} postos. Reduzindo para 5.")
            MIN_POSTOS_POR_CIDADE = 5
            city_grouped_confiavel = city_grouped[city_grouped['NUMERO_DE_POSTOS_PESQUISADOS'] >= MIN_POSTOS_POR_CIDADE].copy()
        
        if city_grouped_confiavel.empty:
            logger.warning("Usando todas as cidades (sem filtro de postos por cidade)")
            city_grouped_confiavel = city_grouped.copy()
        
        logger.info(f"Cidades confiáveis (≥{MIN_POSTOS_POR_CIDADE} postos totais): {len(city_grouped_confiavel)} cidades")
        
        # **DEBUG: Mostrar top 5 cidades mais baratas**
        logger.info("DEBUG - Top 5 cidades mais baratas (após filtros):")
        top5 = city_grouped_confiavel.nsmallest(5, 'PRECO_MEDIO_REVENDA')
        for _, row in top5.iterrows():
            logger.info(f"  {row['MUNICIPIO']}, {row['ESTADO']}: R${row['PRECO_MEDIO_REVENDA']:.2f} ({row['NUMERO_DE_POSTOS_PESQUISADOS']} postos)")
        
        # 4. Encontrar MELHOR preço (menor preço médio em cidade confiável)
        if city_grouped_confiavel.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhuma cidade com dados confiáveis para {fuel_type.value}"
            )
        
        best_idx = city_grouped_confiavel['PRECO_MEDIO_REVENDA'].idxmin()
        best_row = city_grouped_confiavel.loc[best_idx]
        
        best_price = {
            'price': float(best_row['PRECO_MEDIO_REVENDA']),
            'city': str(best_row['MUNICIPIO']),
            'state': str(best_row['ESTADO']),
            'region': str(best_row['REGIAO']),
            'stations_count': int(best_row['NUMERO_DE_POSTOS_PESQUISADOS']),
            'fuel_type': fuel_type.value,
            'price_band': 'BAIXO',
            'price_type': 'average',
            'reliability': 'high' if best_row['NUMERO_DE_POSTOS_PESQUISADOS'] >= 10 else 'medium'
        }
        
        # 5. Encontrar PIOR preço (maior preço médio)
        worst_idx = city_grouped_confiavel['PRECO_MEDIO_REVENDA'].idxmax()
        worst_row = city_grouped_confiavel.loc[worst_idx]
        
        worst_price = {
            'price': float(worst_row['PRECO_MEDIO_REVENDA']),
            'city': str(worst_row['MUNICIPIO']),
            'state': str(worst_row['ESTADO']),
            'region': str(worst_row['REGIAO']),
            'stations_count': int(worst_row['NUMERO_DE_POSTOS_PESQUISADOS'])
        }
        
        # 6. Economia potencial
        potential_saving = worst_price['price'] - best_price['price']
        
        # 7. Total de postos analisados (apenas cidades confiáveis)
        total_stations = int(city_grouped_confiavel['NUMERO_DE_POSTOS_PESQUISADOS'].sum())
        
        # 8. Média nacional (apenas cidades confiáveis)
        national_average = float(city_grouped_confiavel['PRECO_MEDIO_REVENDA'].mean())
        
        # 9. Ranking (top 10 cidades mais baratas confiáveis)
        ranking_cities = city_grouped_confiavel.nsmallest(10, 'PRECO_MEDIO_REVENDA').reset_index()
        
        ranking = []
        for i, row in ranking_cities.iterrows():
            coords = processor._estimate_coordinates(row['MUNICIPIO'], row['ESTADO'])
            
            ranking.append({
                'rank': i + 1,
                'city': row['MUNICIPIO'],
                'state': row['ESTADO'],
                'region': processor._normalize_region(row['REGIAO']),
                'price': float(row['PRECO_MEDIO_REVENDA']),
                'stations': int(row['NUMERO_DE_POSTOS_PESQUISADOS']),
                'latitude': coords['latitude'],
                'longitude': coords['longitude'],
                'reliability': 'high' if row['NUMERO_DE_POSTOS_PESQUISADOS'] >= 10 else 'medium'
            })
        
        # 10. Estatísticas de confiabilidade
        total_cities_analyzed = len(city_grouped_confiavel)
        avg_postos_per_city = total_stations / total_cities_analyzed if total_cities_analyzed > 0 else 0
        
        logger.info(f"RESUMO: {total_cities_analyzed} cidades analisadas, {total_stations} postos totais")
        logger.info(f"  Melhor preço: {best_price['city']} - R${best_price['price']:.2f}")
        logger.info(f"  Pior preço: {worst_price['city']} - R${worst_price['price']:.2f}")
        logger.info(f"  Média nacional: R${national_average:.2f}")
        
        return SummaryResponse(
            best_price=best_price,
            worst_price=worst_price,
            potential_saving=round(potential_saving, 3),
            total_stations=total_stations,
            analysis_date=latest_date if latest_date else datetime.now(),
            ranking=ranking,
            national_average=round(national_average, 3),
            data_date=latest_date,
            # Informações extras de confiabilidade
            reliability_info={
                'min_stations_per_record': MIN_POSTOS_CONFIAVEL,
                'min_stations_per_city': MIN_POSTOS_POR_CIDADE,
                'cities_analyzed': total_cities_analyzed,
                'avg_stations_per_city': round(avg_postos_per_city, 1)
            }
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
