from fastapi import APIRouter, Query, HTTPException
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from app.services.anp_downloader import ANPDownloader
from app.services.data_processor import DataProcessor
from app.services.cache_manager import cache
from app.models.schemas import TrendAnalysis, FuelType
from app.utils.column_helper import get_column_mapping, normalize_city_name  # ADICIONAR

router = APIRouter()
logger = logging.getLogger(__name__)

def get_processor():
    """Obter processador de dados"""
    from app.routes.today import get_processor as get_global_processor
    return get_global_processor()

@router.get("/analysis", response_model=TrendAnalysis)
async def analyze_trend(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível"),
    lookback_days: int = Query(90, ge=7, le=365, description="Período para análise em dias")
):
    """Analisa tendência de preços e gera recomendação"""
    try:
        processor = get_processor()
        df = processor.df
        
        # Usar helper para mapeamento correto
        col_map = get_column_mapping(df)
        
        # Filtrar dados do combustível
        fuel_type_normalized = fuel_type.value.upper()
        fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.upper() == fuel_type_normalized]
        
        if fuel_df.empty:
            # Tentar variações para diesel
            if fuel_type.value == 'diesel':
                fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.contains('DIESEL', case=False, na=False)]
            elif fuel_type.value == 'diesel_s10':
                fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.contains('DIESEL_S10', case=False, na=False)]
            
            if fuel_df.empty:
                raise HTTPException(
                    status_code=404,
                    detail=f"Nenhum dado encontrado para {fuel_type.value}"
                )
        
        # Calcular estatísticas
        current_price = float(fuel_df[col_map['preco_medio_revenda']].mean())
        price_std = float(fuel_df[col_map['preco_medio_revenda']].std())
        volatility = price_std / current_price if current_price > 0 else 0
        
        # Determinar tendência (simulação)
        # Em produção: analisar variação temporal
        trend_indicator = _calculate_trend_indicator(fuel_df, col_map['preco_medio_revenda'], lookback_days)
        
        # Gerar recomendação
        recommendation, reason = _generate_recommendation(
            current_price, volatility, trend_indicator
        )
        
        # Calcular nível de confiança
        confidence_level = _calculate_confidence_level(
            len(fuel_df), volatility, lookback_days
        )
        
        return TrendAnalysis(
            current_price=round(current_price, 3),
            volatility=round(volatility, 4),
            recommendation=recommendation,
            reason=reason,
            analysis_date=datetime.now(),
            trend_indicator=round(trend_indicator, 2),
            confidence_level=round(confidence_level, 1)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")

@router.get("/history")
async def get_price_history(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível"),
    days: int = Query(30, ge=7, le=180, description="Período histórico em dias")
):
    """Retorna histórico de preços (simplificado)"""
    try:
        processor = get_processor()
        df = processor.df
        
        # Usar helper para mapeamento correto
        col_map = get_column_mapping(df)
        
        # Filtrar dados do combustível
        fuel_type_normalized = fuel_type.value.upper()
        fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.upper() == fuel_type_normalized]
        
        if fuel_df.empty:
            # Tentar variações para diesel
            if fuel_type.value == 'diesel':
                fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.contains('DIESEL', case=False, na=False)]
            elif fuel_type.value == 'diesel_s10':
                fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.contains('DIESEL_S10', case=False, na=False)]
            
            if fuel_df.empty:
                raise HTTPException(
                    status_code=404,
                    detail=f"Nenhum dado encontrado para {fuel_type.value}"
                )
        
        # Em produção: buscar dados históricos reais
        # Por enquanto, simulamos com os dados atuais
        
        # Verificar se temos coluna de data
        data_col = None
        for col in df.columns:
            if 'data' in col.lower() and 'inicial' in col.lower():
                data_col = col
                break
        
        if data_col:
            # Tentar usar dados temporais reais
            try:
                # Converter para datetime se necessário
                if not pd.api.types.is_datetime64_any_dtype(fuel_df[data_col]):
                    fuel_df[data_col] = pd.to_datetime(fuel_df[data_col], errors='coerce')
                
                # Filtrar últimos N dias
                cutoff_date = datetime.now() - timedelta(days=days)
                recent_df = fuel_df[fuel_df[data_col] >= cutoff_date]
                
                if not recent_df.empty:
                    # Agrupar por data
                    recent_df['data_dia'] = recent_df[data_col].dt.date
                    grouped = recent_df.groupby('data_dia')[col_map['preco_medio_revenda']].mean().reset_index()
                    grouped = grouped.sort_values('data_dia')
                    
                    history = []
                    for _, row in grouped.iterrows():
                        history.append({
                            'date': row['data_dia'].isoformat(),
                            'price': float(row[col_map['preco_medio_revenda']]),
                            'volume': len(recent_df[recent_df['data_dia'] == row['data_dia']])
                        })
                    
                    if history:
                        current_price = float(fuel_df[col_map['preco_medio_revenda']].mean())
                        return {
                            'fuel_type': fuel_type.value,
                            'period_days': days,
                            'current_price': round(current_price, 3),
                            'history': history,
                            'stats': {
                                'min': round(min(h['price'] for h in history), 3),
                                'max': round(max(h['price'] for h in history), 3),
                                'avg': round(sum(h['price'] for h in history) / len(history), 3),
                                'volatility': round(np.std([h['price'] for h in history]) / current_price, 4)
                            },
                            'source': 'real_data'
                        }
            except Exception as e:
                logger.warning(f"Não foi possível usar dados temporais: {e}")
        
        # Se não conseguiu dados temporais, simular
        current_price = float(fuel_df[col_map['preco_medio_revenda']].mean())
        base_date = datetime.now() - timedelta(days=days)
        
        # Gerar série temporal simulada
        history = []
        for i in range(days, -1, -1):
            date = base_date + timedelta(days=i)
            
            # Simular variação de preço
            variation = np.random.normal(0, 0.002)  # Variação diária pequena
            simulated_price = current_price * (1 + variation * i)
            
            # Adicionar ruído
            noise = np.random.normal(0, 0.01)
            simulated_price *= (1 + noise)
            
            history.append({
                'date': date.date().isoformat(),
                'price': round(simulated_price, 3),
                'volume': np.random.randint(100, 1000)  # Volume simulado
            })
        
        return {
            'fuel_type': fuel_type.value,
            'period_days': days,
            'current_price': round(current_price, 3),
            'history': history,
            'stats': {
                'min': round(min(h['price'] for h in history), 3),
                'max': round(max(h['price'] for h in history), 3),
                'avg': round(sum(h['price'] for h in history) / len(history), 3),
                'volatility': round(np.std([h['price'] for h in history]) / current_price, 4)
            },
            'source': 'simulated_data'
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")

@router.get("/volatility")
async def get_volatility_analysis(
    fuel_type: FuelType = Query(FuelType.GASOLINA, description="Tipo de combustível")
):
    """Analisa volatilidade dos preços"""
    try:
        processor = get_processor()
        df = processor.df
        
        # Usar helper para mapeamento correto
        col_map = get_column_mapping(df)
        
        # Filtrar dados do combustível
        fuel_type_normalized = fuel_type.value.upper()
        fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.upper() == fuel_type_normalized]
        
        if fuel_df.empty:
            # Tentar variações para diesel
            if fuel_type.value == 'diesel':
                fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.contains('DIESEL', case=False, na=False)]
            elif fuel_type.value == 'diesel_s10':
                fuel_df = df[df[col_map['produto_consolidado']].astype(str).str.contains('DIESEL_S10', case=False, na=False)]
            
            if fuel_df.empty:
                raise HTTPException(
                    status_code=404,
                    detail=f"Nenhum dado encontrado para {fuel_type.value}"
                )
        
        prices = fuel_df[col_map['preco_medio_revenda']].values
        
        # Calcular métricas de volatilidade
        std_dev = float(np.std(prices))
        mean_price = float(np.mean(prices))
        cv = std_dev / mean_price if mean_price > 0 else 0
        
        # Classificar volatilidade
        if cv < 0.05:
            volatility_level = "baixa"
        elif cv < 0.1:
            volatility_level = "moderada"
        else:
            volatility_level = "alta"
        
        # Calcular range
        price_range = {
            'min': float(np.min(prices)),
            'max': float(np.max(prices)),
            'range': float(np.max(prices) - np.min(prices))
        }
        
        # Percentis
        percentiles = {
            'p10': float(np.percentile(prices, 10)),
            'p25': float(np.percentile(prices, 25)),
            'p50': float(np.percentile(prices, 50)),
            'p75': float(np.percentile(prices, 75)),
            'p90': float(np.percentile(prices, 90))
        }
        
        return {
            'fuel_type': fuel_type.value,
            'volatility': {
                'coefficient_of_variation': round(cv, 4),
                'level': volatility_level,
                'standard_deviation': round(std_dev, 3)
            },
            'price_distribution': price_range,
            'percentiles': percentiles,
            'analysis': _generate_volatility_analysis(cv, price_range),
            'sample_size': len(prices)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro em /volatility: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")

def _calculate_trend_indicator(fuel_df, price_column, lookback_days):
    """Calcula indicador de tendência (simplificado)"""
    # Em produção: calcular variação real ao longo do tempo
    # Por enquanto, simulamos com base na distribuição atual
    
    prices = fuel_df[price_column].values
    
    # Simular tendência baseada na assimetria dos dados
    if len(prices) > 1 and np.std(prices) > 0:
        skewness = float((np.mean(prices) - np.median(prices)) / np.std(prices))
    else:
        skewness = 0
    
    # Converter para escala -100 a 100
    trend = skewness * 50  # Amplificar o efeito
    
    # Limitar range
    return max(-100, min(100, trend))

def _generate_recommendation(current_price, volatility, trend_indicator):
    """Gera recomendação baseada na análise"""
    
    # Avaliar condições
    conditions = []
    
    if volatility > 0.08:
        conditions.append(("alta volatilidade", 2))
    elif volatility > 0.05:
        conditions.append(("volatilidade moderada", 1))
    
    if trend_indicator > 5:
        conditions.append(("tendência de alta", 2))
    elif trend_indicator < -5:
        conditions.append(("tendência de baixa", -1))
    
    if not conditions:
        return "pode_esperar", "Preços estáveis sem tendência definida"
    
    # Calcular score
    score = sum(weight for _, weight in conditions)
    
    if score >= 3:
        return "abastecer_agora", "Risco significativo de aumento nos preços"
    elif score >= 1:
        return "abastecer_agora", "Condições favoráveis para abastecimento imediato"
    elif score <= -2:
        return "pode_esperar", "Possibilidade de redução nos preços"
    else:
        return "pode_esperar", "Situação neutra, pode aguardar por oportunidades"

def _calculate_confidence_level(sample_size, volatility, lookback_days):
    """Calcula nível de confiança da análise"""
    
    # Baseado no tamanho da amostra
    sample_confidence = min(100, (sample_size / 1000) * 100)
    
    # Baseado na volatilidade (mais volátil = menor confiança)
    volatility_penalty = volatility * 200  # Penalidade de 0-20%
    
    # Baseado no período (períodos mais longos = mais confiança)
    period_boost = min(30, lookback_days / 90 * 30)
    
    confidence = sample_confidence - volatility_penalty + period_boost
    
    return max(0, min(100, confidence))

def _generate_volatility_analysis(cv, price_range):
    """Gera análise textual da volatilidade"""
    
    if cv < 0.03:
        return {
            'level': 'MUITO BAIXA',
            'description': 'Preços muito estáveis. Pouca variação entre regiões.',
            'implication': 'Pode abastecer em qualquer lugar sem grande prejuízo.'
        }
    elif cv < 0.06:
        return {
            'level': 'BAIXA',
            'description': 'Preços relativamente estáveis. Variação moderada.',
            'implication': 'Vale pesquisar, mas diferenças são pequenas.'
        }
    elif cv < 0.1:
        return {
            'level': 'MODERADA',
            'description': 'Volatilidade significativa. Preços variam consideravelmente.',
            'implication': 'Importante comparar preços antes de abastecer.'
        }
    elif cv < 0.15:
        return {
            'level': 'ALTA',
            'description': 'Alta volatilidade. Grandes diferenças de preço.',
            'implication': 'Oportunidades de economia significativa. Pesquise bem.'
        }
    else:
        return {
            'level': 'MUITO ALTA',
            'description': 'Extrema volatilidade. Preços muito instáveis.',
            'implication': 'Risco/oportunidade alto. Abasteça estrategicamente.'
        }
