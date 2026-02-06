import os
from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    # Configurações da aplicação
    APP_NAME: str = "FuelMetrics"
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # Banco de dados (Railway fornece DATABASE_URL automaticamente)
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./fuelmetrics.db")
    
    # Cache
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "3600"))
    REDIS_URL: Optional[str] = os.getenv("REDIS_URL")
    
    # ANP
    ANP_BASE_URL: str = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal"
    ANP_UPDATE_INTERVAL_DAYS: int = int(os.getenv("ANP_UPDATE_INTERVAL_DAYS", "7"))
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/fuelmetrics.log")
    
    # API
    API_PREFIX: str = "/api"
    API_VERSION: str = "v1"
    
    # CORS
    CORS_ORIGINS: str = os.getenv("CORS_ORIGINS", "*")
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()