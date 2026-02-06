from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from app.config import settings

# Criar engine do banco de dados
if settings.DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        settings.DATABASE_URL,
        connect_args={"check_same_thread": False}
    )
else:
    engine = create_engine(settings.DATABASE_URL)

# Criar session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para modelos
Base = declarative_base()

class FuelPrice(Base):
    __tablename__ = "fuel_prices"
    
    id = Column(Integer, primary_key=True, index=True)
    municipio = Column(String(100), index=True)
    estado = Column(String(2), index=True)
    regiao = Column(String(20), index=True)
    produto = Column(String(50), index=True)
    produto_consolidado = Column(String(50), index=True)
    preco_medio_revenda = Column(Float)
    numero_de_postos_pesquisados = Column(Integer)
    data_coleta = Column(DateTime)
    data_referencia = Column(DateTime)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f"<FuelPrice {self.municipio} - {self.produto}: R${self.preco_medio_revenda}>"

class CacheMetadata(Base):
    __tablename__ = "cache_metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(100), unique=True, index=True)
    value = Column(Text)
    expires_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f"<CacheMetadata {self.key}>"

class AnalysisLog(Base):
    __tablename__ = "analysis_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    analysis_type = Column(String(50), index=True)
    parameters = Column(Text)
    result = Column(Text)
    execution_time_ms = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<AnalysisLog {self.analysis_type} at {self.created_at}>"

# Função para obter sessão do banco
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Função para criar tabelas
def create_tables():
    Base.metadata.create_all(bind=engine)

# Função para dropar tabelas (apenas desenvolvimento)
def drop_tables():
    Base.metadata.drop_all(bind=engine)

if __name__ == "__main__":
    create_tables()
    print("Tabelas criadas com sucesso!")
