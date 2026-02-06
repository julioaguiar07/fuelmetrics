"""
Helper para lidar com inconsistências nos nomes das colunas
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_column_mapping(df: pd.DataFrame) -> dict:
    """
    Retorna mapeamento flexível de colunas para lidar com:
    - MAIÚSCULAS vs minúsculas
    - Com vs sem acentos
    - Espaços vs underscores
    """
    mapping = {}
    
    # Lista de colunas que precisamos
    target_columns = {
        'municipio': ['municipio', 'município', 'municipality', 'city'],
        'estado': ['estado', 'state', 'uf'],
        'regiao': ['regiao', 'região', 'region'],
        'produto': ['produto', 'product', 'fuel'],
        'produto_consolidado': ['produto_consolidado', 'produto consolidado', 'consolidated_product'],
        'preco_medio_revenda': ['preco medio revenda', 'preço médio revenda', 'price'],
        'numero_de_postos_pesquisados': ['numero de postos pesquisados', 'número de postos pesquisados', 'stations']
    }
    
    # Converter todas as colunas para minúsculas sem acentos para comparação
    df_columns_lower = {}
    for col in df.columns:
        normalized = col.lower()
        # Remover acentos
        normalized = normalized.replace('á', 'a').replace('à', 'a').replace('â', 'a').replace('ã', 'a')
        normalized = normalized.replace('é', 'e').replace('ê', 'e')
        normalized = normalized.replace('í', 'i').replace('î', 'i')
        normalized = normalized.replace('ó', 'o').replace('ô', 'o').replace('õ', 'o')
        normalized = normalized.replace('ú', 'u').replace('û', 'u')
        normalized = normalized.replace('ç', 'c')
        normalized = normalized.replace(' ', '_')  # Normalizar espaços para underscores
        df_columns_lower[col] = normalized
    
    # Para cada coluna alvo, encontrar a correspondente no DataFrame
    for target_key, possible_names in target_columns.items():
        found = False
        
        # Primeiro tentar match exato (com normalização)
        for possible_name in possible_names:
            possible_normalized = possible_name.lower()
            # Remover acentos do possible_name também
            possible_normalized = possible_normalized.replace('á', 'a').replace('à', 'a').replace('â', 'a').replace('ã', 'a')
            possible_normalized = possible_normalized.replace('é', 'e').replace('ê', 'e')
            possible_normalized = possible_normalized.replace('í', 'i').replace('î', 'i')
            possible_normalized = possible_normalized.replace('ó', 'o').replace('ô', 'o').replace('õ', 'o')
            possible_normalized = possible_normalized.replace('ú', 'u').replace('û', 'u')
            possible_normalized = possible_normalized.replace('ç', 'c')
            possible_normalized = possible_normalized.replace(' ', '_')
            
            for df_col, df_normalized in df_columns_lower.items():
                if possible_normalized in df_normalized or df_normalized in possible_normalized:
                    mapping[target_key] = df_col
                    found = True
                    logger.debug(f"Mapeado '{target_key}' -> '{df_col}'")
                    break
            
            if found:
                break
        
        # Se não encontrou, tentar heurística
        if not found:
            for df_col in df.columns:
                df_col_lower = df_col.lower()
                if target_key in df_col_lower or any(keyword in df_col_lower for keyword in target_key.split('_')):
                    mapping[target_key] = df_col
                    found = True
                    logger.debug(f"Mapeado heurístico '{target_key}' -> '{df_col}'")
                    break
        
        # Último recurso: usar primeira coluna que contém parte do nome
        if not found and target_key == 'municipio':
            for df_col in df.columns:
                if 'muni' in df_col.lower():
                    mapping[target_key] = df_col
                    found = True
                    logger.warning(f"Fallback mapeamento '{target_key}' -> '{df_col}'")
                    break
    
    # Log final
    logger.info(f"Mapeamento de colunas: {mapping}")
    
    return mapping

def normalize_city_name(city: str) -> str:
    """Normaliza nome da cidade para busca"""
    if not isinstance(city, str):
        return ""
    
    # Converter para maiúsculas
    normalized = city.upper()
    
    # Remover acentos
    normalized = normalized.replace('Á', 'A').replace('À', 'A').replace('Â', 'A').replace('Ã', 'A')
    normalized = normalized.replace('É', 'E').replace('Ê', 'E')
    normalized = normalized.replace('Í', 'I').replace('Î', 'I')
    normalized = normalized.replace('Ó', 'O').replace('Ô', 'O').replace('Õ', 'O')
    normalized = normalized.replace('Ú', 'U').replace('Û', 'U')
    normalized = normalized.replace('Ç', 'C')
    
    # Remover espaços extras
    normalized = ' '.join(normalized.split())
    
    return normalized
