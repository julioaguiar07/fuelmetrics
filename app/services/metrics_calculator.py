import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

class MetricsCalculator:
    """Calculadora de métricas avançadas para análise de preços"""
    
    @staticmethod
    def calculate_price_dispersion(prices: List[float]) -> Dict:
        """Calcula dispersão dos preços"""
        if not prices:
            return {}
        
        prices_array = np.array(prices)
        
        return {
            'range': float(np.max(prices_array) - np.min(prices_array)),
            'interquartile_range': float(
                np.percentile(prices_array, 75) - np.percentile(prices_array, 25)
            ),
            'coefficient_of_variation': float(
                np.std(prices_array) / np.mean(prices_array) 
                if np.mean(prices_array) > 0 else 0
            ),
            'gini_coefficient': MetricsCalculator._calculate_gini(prices_array),
            'price_elasticity': MetricsCalculator._calculate_price_elasticity(prices_array)
        }
    
    @staticmethod
    def _calculate_gini(prices: np.ndarray) -> float:
        """Calcula coeficiente de Gini para desigualdade de preços"""
        # Ordenar preços
        sorted_prices = np.sort(prices)
        n = len(sorted_prices)
        
        # Calcular índice de Gini
        index = np.arange(1, n + 1)
        return float((np.sum((2 * index - n - 1) * sorted_prices)) / (n * np.sum(sorted_prices)))
    
    @staticmethod
    def _calculate_price_elasticity(prices: np.ndarray) -> float:
        """Calcula elasticidade de preços (simplificado)"""
        if len(prices) < 2:
            return 0.0
        
        # Calcular variação percentual média
        percent_changes = np.diff(prices) / prices[:-1]
        return float(np.mean(np.abs(percent_changes)))
    
    @staticmethod
    def calculate_regional_comparison(
        region_prices: Dict[str, List[float]]
    ) -> Dict[str, Dict]:
        """Compara preços entre regiões"""
        results = {}
        
        for region, prices in region_prices.items():
            if not prices:
                continue
            
            prices_array = np.array(prices)
            
            results[region] = {
                'mean': float(np.mean(prices_array)),
                'median': float(np.median(prices_array)),
                'std': float(np.std(prices_array)),
                'min': float(np.min(prices_array)),
                'max': float(np.max(prices_array)),
                'count': len(prices_array),
                'percentile_25': float(np.percentile(prices_array, 25)),
                'percentile_75': float(np.percentile(prices_array, 75))
            }
        
        # Calcular comparações relativas
        if results:
            national_mean = np.mean([r['mean'] for r in results.values()])
            
            for region in results:
                results[region]['relative_to_national'] = (
                    results[region]['mean'] / national_mean * 100 - 100
                )
        
        return results
    
    @staticmethod
    def calculate_economic_indicators(
        prices: List[float],
        reference_price: float
    ) -> Dict:
        """Calcula indicadores econômicos"""
        if not prices:
            return {}
        
        prices_array = np.array(prices)
        
        # Calcular sobrepreço médio
        overprice = prices_array - reference_price
        overprice_percentage = (overprice / reference_price) * 100
        
        return {
            'average_overprice': float(np.mean(overprice)),
            'median_overprice': float(np.median(overprice)),
            'overprice_percentage_mean': float(np.mean(overprice_percentage)),
            'overprice_percentage_median': float(np.median(overprice_percentage)),
            'consumption_cost_per_1000km': MetricsCalculator._calculate_consumption_cost(
                prices_array, reference_price
            ),
            'potential_savings': MetricsCalculator._calculate_potential_savings(
                prices_array, reference_price
            )
        }
    
    @staticmethod
    def _calculate_consumption_cost(
        prices: np.ndarray,
        reference_price: float,
        consumption_km_l: float = 12.0
    ) -> Dict:
        """Calcula custo de consumo para 1000 km"""
        avg_price = np.mean(prices)
        liters_per_1000km = 1000 / consumption_km_l
        
        return {
            'at_average_price': float(liters_per_1000km * avg_price),
            'at_reference_price': float(liters_per_1000km * reference_price),
            'difference': float(liters_per_1000km * (avg_price - reference_price))
        }
    
    @staticmethod
    def _calculate_potential_savings(
        prices: np.ndarray,
        reference_price: float,
        monthly_consumption_l: float = 100.0
    ) -> Dict:
        """Calcula economia potencial"""
        avg_price = np.mean(prices)
        
        monthly_savings = monthly_consumption_l * (avg_price - reference_price)
        annual_savings = monthly_savings * 12
        
        return {
            'monthly': float(monthly_savings),
            'annual': float(annual_savings),
            'percentage': float((avg_price - reference_price) / reference_price * 100)
        }
    
    @staticmethod
    def detect_price_clusters(
        prices: List[float],
        n_clusters: int = 3
    ) -> List[Dict]:
        """Detecta clusters de preços usando K-means simplificado"""
        if len(prices) < n_clusters:
            return []
        
        from sklearn.cluster import KMeans
        import numpy as np
        
        prices_array = np.array(prices).reshape(-1, 1)
        
        # Aplicar K-means
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        kmeans.fit(prices_array)
        
        clusters = []
        for i in range(n_clusters):
            cluster_prices = prices_array[kmeans.labels_ == i].flatten()
            
            if len(cluster_prices) > 0:
                clusters.append({
                    'cluster_id': i,
                    'center': float(kmeans.cluster_centers_[i][0]),
                    'size': len(cluster_prices),
                    'min': float(np.min(cluster_prices)),
                    'max': float(np.max(cluster_prices)),
                    'mean': float(np.mean(cluster_prices)),
                    'std': float(np.std(cluster_prices))
                })
        
        # Ordenar por centro do cluster
        clusters.sort(key=lambda x: x['center'])
        
        return clusters
    
    @staticmethod
    def calculate_trend_metrics(
        historical_data: List[Dict],  # Lista de {date: str, price: float}
        window_days: int = 7
    ) -> Dict:
        """Calcula métricas de tendência temporal"""
        if len(historical_data) < 2:
            return {}
        
        # Converter para DataFrame
        df = pd.DataFrame(historical_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Calcular variações
        df['price_change'] = df['price'].pct_change()
        df['rolling_mean'] = df['price'].rolling(window=min(window_days, len(df))).mean()
        df['rolling_std'] = df['price'].rolling(window=min(window_days, len(df))).std()
        
        # Calcular métricas
        volatility = df['price_change'].std() if len(df) > 1 else 0
        
        # Determinar tendência
        if len(df) >= 3:
            recent = df['price'].iloc[-3:].mean()
            older = df['price'].iloc[:3].mean()
            trend = (recent - older) / older * 100
        else:
            trend = 0
        
        return {
            'volatility': float(volatility),
            'trend_percentage': float(trend),
            'trend_direction': 'up' if trend > 0 else 'down' if trend < 0 else 'stable',
            'current_price': float(df['price'].iloc[-1]) if len(df) > 0 else 0,
            'price_change_7d': float(
                (df['price'].iloc[-1] - df['price'].iloc[-8]) / df['price'].iloc[-8] * 100
            ) if len(df) >= 8 else 0,
            'price_change_30d': float(
                (df['price'].iloc[-1] - df['price'].iloc[-31]) / df['price'].iloc[-31] * 100
            ) if len(df) >= 31 else 0,
            'analysis_period': {
                'start_date': df['date'].iloc[0].isoformat() if len(df) > 0 else None,
                'end_date': df['date'].iloc[-1].isoformat() if len(df) > 0 else None,
                'days': len(df)
            }
        }
