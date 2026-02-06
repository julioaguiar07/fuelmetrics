from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class FuelType(str, Enum):
    GASOLINA = "gasolina"
    DIESEL = "diesel"
    DIESEL_S10 = "diesel_s10"
    GNV = "gnv"

class Region(str, Enum):
    NORTE = "NORTE"
    NORDESTE = "NORDESTE"
    CENTRO_OESTE = "CENTRO_OESTE"
    SUDESTE = "SUDESTE"
    SUL = "SUL"

class BestPriceResponse(BaseModel):
    price: float = Field(..., description="Preço do combustível em R$/L")
    city: str = Field(..., description="Nome do município")
    state: str = Field(..., description="Sigla do estado")
    region: Region = Field(..., description="Região do Brasil")
    fuel_type: FuelType = Field(..., description="Tipo de combustível")
    stations_count: int = Field(..., description="Número de postos pesquisados")
    latitude: Optional[float] = Field(None, description="Latitude aproximada")
    longitude: Optional[float] = Field(None, description="Longitude aproximada")
    
    @validator('price')
    def validate_price(cls, v):
        if v <= 0:
            raise ValueError('Preço deve ser maior que zero')
        return round(v, 3)

class RankingItem(BaseModel):
    rank: int = Field(..., ge=1, description="Posição no ranking")
    city: str = Field(..., description="Nome do município")
    state: str = Field(..., description="Sigla do estado")
    region: Region = Field(..., description="Região do Brasil")
    price: float = Field(..., description="Preço médio em R$/L")
    stations: int = Field(..., description="Número de postos")
    distance_km: Optional[float] = Field(None, description="Distância em km (se aplicável)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "rank": 1,
                "city": "POÇOS DE CALDAS",
                "state": "MG",
                "region": "SUDESTE",
                "price": 4.89,
                "stations": 47
            }
        }

class RegionStats(BaseModel):
    region: Region = Field(..., description="Região do Brasil")
    fuel_type: FuelType = Field(..., description="Tipo de combustível")
    avg_price: float = Field(..., description="Preço médio na região")
    min_price: float = Field(..., description="Preço mínimo na região")
    max_price: float = Field(..., description="Preço máximo na região")
    city_count: int = Field(..., description="Número de cidades na região")
    stations_count: int = Field(..., description="Total de postos na região")
    color_index: float = Field(..., ge=0, le=100, description="Índice para escala de cores (0-100)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "region": "SUDESTE",
                "fuel_type": "gasolina",
                "avg_price": 5.12,
                "min_price": 4.89,
                "max_price": 5.89,
                "city_count": 156,
                "stations_count": 5247,
                "color_index": 45.3
            }
        }

class CityComparison(BaseModel):
    city: str = Field(..., description="Nome do município")
    state: str = Field(..., description="Sigla do estado")
    region: Region = Field(..., description="Região do Brasil")
    fuels: Dict[str, Dict[str, Any]] = Field(..., description="Dados por tipo de combustível")
    overall_stats: Dict[str, Any] = Field(..., description="Estatísticas gerais")
    recommendation: Optional[str] = Field(None, description="Recomendação (recommended/not_recommended)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "city": "SÃO PAULO",
                "state": "SP",
                "region": "SUDESTE",
                "fuels": {
                    "gasolina": {
                        "avg": 5.67,
                        "min": 5.23,
                        "max": 6.15,
                        "stations": 1247
                    }
                }
            }
        }

class TrendAnalysis(BaseModel):
    current_price: float = Field(..., description="Preço atual médio")
    volatility: float = Field(..., ge=0, description="Volatilidade dos preços")
    recommendation: str = Field(..., description="Recomendação: abastecer_agora/pode_esperar")
    reason: str = Field(..., description="Justificativa da recomendação")
    analysis_date: datetime = Field(..., description="Data da análise")
    trend_indicator: float = Field(..., description="Indicador de tendência (-100 a 100)")
    confidence_level: float = Field(..., ge=0, le=100, description="Nível de confiança da análise (%)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "current_price": 5.29,
                "volatility": 0.083,
                "recommendation": "abastecer_agora",
                "reason": "Preços em tendência de alta com alta volatilidade",
                "analysis_date": "2026-02-06T10:30:00Z",
                "trend_indicator": 8.3,
                "confidence_level": 85.5
            }
        }

class SimulatorRequest(BaseModel):
    tank_capacity: float = Field(..., gt=0, description="Capacidade do tanque em litros")
    current_level: float = Field(..., ge=0, le=100, description="Nível atual do tanque em %")
    consumption: float = Field(..., gt=0, description="Consumo médio em km/L")
    distance: float = Field(..., gt=0, description="Distância da viagem em km")
    fuel_type: FuelType = Field(FuelType.GASOLINA, description="Tipo de combustível")
    fuel_price: Optional[float] = Field(None, description="Preço do combustível (opcional)")
    
    @validator('current_level')
    def validate_current_level(cls, v):
        if v < 0 or v > 100:
            raise ValueError('Nível atual deve estar entre 0 e 100%')
        return v

class SimulatorResponse(BaseModel):
    current_autonomy: float = Field(..., description="Autonomia atual em km")
    required_autonomy: float = Field(..., description="Autonomia necessária em km")
    remaining_liters: float = Field(..., description="Litros restantes após viagem")
    remaining_percent: float = Field(..., description="Percentual do tanque restante")
    fuel_needed: float = Field(..., description="Litros necessários para viagem")
    status: str = Field(..., description="Status: safe/warning/danger")
    message: str = Field(..., description="Mensagem descritiva")
    estimated_cost: Optional[float] = Field(None, description="Custo estimado em R$")
    safety_margin: float = Field(..., description="Margem de segurança em km")
    
    class Config:
        json_schema_extra = {
            "example": {
                "current_autonomy": 360.0,
                "required_autonomy": 95.0,
                "remaining_liters": 22.1,
                "remaining_percent": 44.2,
                "fuel_needed": 7.9,
                "status": "safe",
                "message": "Viagem segura com combustível suficiente",
                "estimated_cost": 40.45,
                "safety_margin": 265.0
            }
        }

class SummaryResponse(BaseModel):
    best_price: BestPriceResponse = Field(..., description="Melhor preço encontrado")
    worst_price: Dict[str, Any] = Field(..., description="Pior preço encontrado")
    potential_saving: float = Field(..., description="Economia potencial por litro")
    total_stations: int = Field(..., description="Total de postos analisados")
    analysis_date: datetime = Field(..., description="Data da análise")
    ranking: List[RankingItem] = Field(..., description="Top 10 mais baratos")
    national_average: float = Field(..., description="Média nacional")
    
    class Config:
        json_schema_extra = {
            "example": {
                "potential_saving": 0.87,
                "total_stations": 12847,
                "national_average": 5.29
            }
        }

class HealthResponse(BaseModel):
    status: str = Field(..., description="Status do serviço")
    timestamp: datetime = Field(..., description="Timestamp da verificação")
    service: str = Field(..., description="Nome do serviço")
    version: str = Field(..., description="Versão da API")
    database_status: Optional[str] = Field(None, description="Status do banco de dados")
    cache_status: Optional[str] = Field(None, description="Status do cache")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2026-02-06T10:30:00Z",
                "service": "fuelmetrics-api",
                "version": "1.0.0",
                "database_status": "connected",
                "cache_status": "active"
            }
        }
