#!/usr/bin/env python3
import pandas as pd
import requests
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def diagnose_excel():
    """Diagnóstico completo do arquivo Excel da ANP"""
    url = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal/semanal-municipios-2026.xlsx"
    local_path = Path("data/diagnose_anp.xlsx")
    
    # Baixar arquivo
    logger.info(f"Baixando {url}")
    response = requests.get(url)
    with open(local_path, 'wb') as f:
        f.write(response.content)
    
    logger.info(f"Arquivo salvo: {local_path}")
    
    # Tentar diferentes métodos de leitura
    methods = [
        ("read_excel padrão", pd.read_excel),
        ("read_excel sem header", lambda f: pd.read_excel(f, header=None)),
        ("read_excel com engine openpyxl", lambda f: pd.read_excel(f, engine='openpyxl')),
        ("read_excel sheet_name=None", lambda f: pd.read_excel(f, sheet_name=None)),
    ]
    
    for method_name, method in methods:
        try:
            logger.info(f"\n=== Tentando: {method_name} ===")
            df = method(local_path)
            
            if isinstance(df, dict):
                logger.info(f"É um dicionário de sheets: {list(df.keys())}")
                for sheet_name, sheet_df in df.items():
                    logger.info(f"Sheet '{sheet_name}': {len(sheet_df)} linhas, {len(sheet_df.columns)} colunas")
                    if len(sheet_df) > 0:
                        logger.info(f"Primeiras 3 linhas:")
                        for i in range(min(3, len(sheet_df))):
                            logger.info(f"  Linha {i}: {sheet_df.iloc[i].tolist()}")
            else:
                logger.info(f"DataFrame: {len(df)} linhas, {len(df.columns)} colunas")
                logger.info(f"Colunas: {list(df.columns)}")
                
                if len(df) > 0:
                    logger.info("Primeiras 5 linhas:")
                    for i in range(min(5, len(df))):
                        row = df.iloc[i]
                        logger.info(f"  Linha {i}:")
                        for col in df.columns:
                            logger.info(f"    {col}: {row[col]}")
                
                # Mostrar tipos de dados
                logger.info(f"\nTipos de dados:")
                for col in df.columns:
                    logger.info(f"  {col}: {df[col].dtype}")
                    
        except Exception as e:
            logger.error(f"Erro com {method_name}: {e}")
    
    # Análise de bytes do arquivo
    logger.info(f"\n=== Análise de bytes ===")
    with open(local_path, 'rb') as f:
        first_1000 = f.read(1000)
        logger.info(f"Primeiros 1000 bytes (hex): {first_1000[:100].hex()}")
        logger.info(f"Primeiros 1000 bytes (texto): {first_1000[:200]}")
    
    logger.info("\n=== DIAGNÓSTICO COMPLETO ===")

if __name__ == "__main__":
    diagnose_excel()
