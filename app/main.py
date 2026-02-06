from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import logging
import os
from datetime import datetime
from app.config import settings
from app.routes import today, compare, trend, simulator
from app.tasks.scheduler import start_scheduler, shutdown_scheduler
from app.utils.logger import setup_logging

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia ciclo de vida da aplicação"""
    # Inicialização
    logger.info("=" * 60)
    logger.info("Iniciando FuelMetrics API")
    logger.info(f"Ambiente: {settings.ENVIRONMENT}")
    logger.info(f"Data/Hora: {datetime.now()}")
    logger.info("=" * 60)
    
    try:
        start_scheduler()
        logger.info("Agendador iniciado com sucesso")
    except Exception as e:
        logger.error(f"Erro ao iniciar agendador: {e}")
    
    yield
    
    # Shutdown
    logger.info("Desligando FuelMetrics API...")
    shutdown_scheduler()

# Criar aplicação FastAPI
app = FastAPI(
    title="FuelMetrics API",
    description="API para análise de preços de combustíveis da ANP",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Configurar CORS
origins = settings.CORS_ORIGINS.split(",") if "," in settings.CORS_ORIGINS else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar rotas da API
app.include_router(today.router, prefix="/api/today", tags=["today"])
app.include_router(compare.router, prefix="/api/compare", tags=["compare"])
app.include_router(trend.router, prefix="/api/trend", tags=["trend"])
app.include_router(simulator.router, prefix="/api/simulator", tags=["simulator"])

# Servir frontend estático se existir
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")

@app.get("/")
async def root():
    """Página inicial"""
    frontend_index = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
    if os.path.exists(frontend_index):
        return FileResponse(frontend_index)
    return {
        "message": "FuelMetrics API",
        "version": "1.0.0",
        "docs": "/api/docs",
        "health": "/health"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "fuelmetrics-api",
        "timestamp": datetime.now().isoformat(),
        "environment": settings.ENVIRONMENT
    }

@app.get("/api")
async def api_info():
    """API information"""
    return {
        "name": "FuelMetrics API",
        "description": "API para análise de preços de combustíveis da ANP",
        "version": "1.0.0",
        "endpoints": {
            "today": "/api/today",
            "compare": "/api/compare",
            "trend": "/api/trend",
            "simulator": "/api/simulator"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=settings.ENVIRONMENT == "development"
    )
