"""
Helper para lidar com inconsistências nos nomes das colunas
"""

import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)

def remove_accents(text: str) -> str:
    """Remove acentos de uma string"""
    if not isinstance(text, str):
        return text
    
    # Mapeamento de acentos
    accent_map = {
        'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a',
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
        'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
        'ç': 'c', 'ñ': 'n',
        'Á': 'A', 'À': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A',
        'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E',
        'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I',
        'Ó': 'O', 'Ò': 'O', 'Ô': 'O', 'Õ': 'O', 'Ö': 'O',
        'Ú': 'U', 'Ù': 'U', 'Û': 'U', 'Ü': 'U',
        'Ç': 'C', 'Ñ': 'N'
    }
    
    # Substituir caracteres acentuados
    for accented, normal in accent_map.items():
        text = text.replace(accented, normal)
    
    return text

def normalize_column_name(col_name: str) -> str:
    """Normaliza nome de coluna para comparação"""
    if not isinstance(col_name, str):
        col_name = str(col_name)
    
    # Converter para minúsculas e remover acentos
    normalized = col_name.lower()
    normalized = remove_accents(normalized)
    
    # Remover caracteres especiais e espaços
    normalized = re.sub(r'[^a-z0-9]', '_', normalized)
    
    # Remover underscores múltiplos
    normalized = re.sub(r'_+', '_', normalized)
    
    # Remover underscores no início/fim
    normalized = normalized.strip('_')
    
    return normalized

def get_column_mapping(df: pd.DataFrame) -> dict:
    """
    Retorna mapeamento flexível de colunas para lidar com:
    - MAIÚSCULAS vs minúsculas
    - Com vs sem acentos
    - Espaços vs underscores
    """
    mapping = {}
    
    # Primeiro, normalizar todos os nomes de colunas do DataFrame
    df_columns_normalized = {}
    for col in df.columns:
        normalized = normalize_column_name(col)
        df_columns_normalized[normalized] = col
    
    # Mapeamento alvo (o que precisamos encontrar)
    target_mappings = {
        'municipio': ['municipio', 'municipality', 'city', 'cidade'],
        'estado': ['estado', 'state', 'uf', 'unidade_federativa'],
        'regiao': ['regiao', 'region', 'area'],
        'produto': ['produto', 'product', 'fuel', 'combustivel'],
        'produto_consolidado': ['produto_consolidado', 'produto_consolidado', 'consolidated_product'],
        'preco_medio_revenda': ['preco_medio_revenda', 'preco_medio', 'price', 'valor_medio'],
        'numero_de_postos_pesquisados': ['numero_de_postos_pesquisados', 'postos_pesquisados', 'stations', 'numero_postos'],
        'data_inicial': ['data_inicial', 'data', 'date', 'periodo', 'mes']
    }
    
    # Para cada coluna alvo, encontrar a correspondente
    for target_key, possible_names in target_mappings.items():
        found_col = None
        
        # Tentar match exato primeiro
        for possible_name in possible_names:
            normalized_possible = normalize_column_name(possible_name)
            if normalized_possible in df_columns_normalized:
                found_col = df_columns_normalized[normalized_possible]
                break
        
        # Se não encontrou, procurar por contém
        if not found_col:
            for df_normalized, df_col in df_columns_normalized.items():
                for possible_name in possible_names:
                    normalized_possible = normalize_column_name(possible_name)
                    if normalized_possible in df_normalized or df_normalized in normalized_possible:
                        found_col = df_col
                        break
                if found_col:
                    break
        
        # Se ainda não encontrou, usar heurística para alguns casos específicos
        if not found_col:
            if target_key == 'produto_consolidado':
                # Procurar por coluna que tem produto consolidado
                for df_col in df.columns:
                    col_normalized = normalize_column_name(df_col)
                    if 'consolidado' in col_normalized:
                        found_col = df_col
                        break
                # Se ainda não, usar coluna produto
                if not found_col and 'produto' in mapping:
                    found_col = mapping['produto']
            
            elif target_key == 'preco_medio_revenda':
                # Procurar qualquer coluna com preço
                for df_col in df.columns:
                    col_normalized = normalize_column_name(df_col)
                    if 'preco' in col_normalized or 'price' in col_normalized or 'valor' in col_normalized:
                        found_col = df_col
                        break
        
        # Atribuir ao mapeamento
        if found_col:
            mapping[target_key] = found_col
        else:
            # Fallback: usar o primeiro que parece ser
            for df_col in df.columns:
                col_normalized = normalize_column_name(df_col)
                if target_key in col_normalized:
                    mapping[target_key] = df_col
                    break
    
    # Log detalhado para debug
    logger.info(f"Mapeamento de colunas encontrado:")
    for key, value in mapping.items():
        logger.info(f"  {key} -> {value}")
    
    # Verificar se temos mapeamento essencial
    essential_cols = ['municipio', 'estado', 'preco_medio_revenda']
    missing = [col for col in essential_cols if col not in mapping]
    if missing:
        logger.warning(f"Colunas essenciais faltando no mapeamento: {missing}")
        logger.warning(f"Colunas disponíveis no DataFrame: {list(df.columns)}")
    
    return mapping

def normalize_city_name(city: str) -> str:
    """Normaliza nome da cidade para busca"""
    if not isinstance(city, str):
        return ""
    
    # Converter para maiúsculas
    normalized = city.upper()
    
    # Remover acentos
    normalized = remove_accents(normalized)
    
    # Remover espaços extras
    normalized = ' '.join(normalized.split())
    
    return normalized

def get_latest_data(df: pd.DataFrame, date_column: str = 'DATA_INICIAL') -> pd.DataFrame:
    """Retorna apenas os dados mais recentes do DataFrame"""
    try:
        # Tentar encontrar coluna de data
        col_map = get_column_mapping(df)
        
        date_col = None
        if 'data_inicial' in col_map:
            date_col = col_map['data_inicial']
        else:
            # Procurar manualmente
            for col in df.columns:
                col_lower = col.lower()
                if 'data' in col_lower or 'date' in col_lower:
                    date_col = col
                    break
        
        if date_col and date_col in df.columns:
            # Converter para datetime se necessário
            if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Encontrar data mais recente
            latest_date = df[date_col].max()
            
            # Filtrar dados mais recentes (última semana)
            if pd.notna(latest_date):
                # Pegar dados da última semana
                cutoff_date = latest_date - pd.Timedelta(days=7)
                latest_data = df[df[date_col] >= cutoff_date].copy()
                
                logger.info(f"Filtrando dados: {len(latest_data)}/{len(df)} registros da última semana")
                logger.info(f"Data mais recente: {latest_date}, corte: {cutoff_date}")
                
                return latest_data
        
        # Se não conseguiu filtrar por data, retornar tudo
        logger.warning("Não foi possível filtrar por data, retornando todos os dados")
        return df.copy()
        
    except Exception as e:
        logger.warning(f"Erro ao filtrar dados por data: {e}")
        return df.copy()
