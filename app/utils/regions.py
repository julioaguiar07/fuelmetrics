"""
Mapeamento de estados para regiões do Brasil
"""

# Mapeamento completo de siglas para regiões
STATE_TO_REGION = {
    # Região Norte
    'AC': 'NORTE',  # Acre
    'AP': 'NORTE',  # Amapá
    'AM': 'NORTE',  # Amazonas
    'PA': 'NORTE',  # Pará
    'RO': 'NORTE',  # Rondônia
    'RR': 'NORTE',  # Roraima
    'TO': 'NORTE',  # Tocantins
    
    # Região Nordeste
    'AL': 'NORDESTE',  # Alagoas
    'BA': 'NORDESTE',  # Bahia
    'CE': 'NORDESTE',  # Ceará
    'MA': 'NORDESTE',  # Maranhão
    'PB': 'NORDESTE',  # Paraíba
    'PE': 'NORDESTE',  # Pernambuco
    'PI': 'NORDESTE',  # Piauí
    'RN': 'NORDESTE',  # Rio Grande do Norte
    'SE': 'NORDESTE',  # Sergipe
    
    # Região Centro-Oeste
    'DF': 'CENTRO-OESTE',  # Distrito Federal
    'GO': 'CENTRO-OESTE',  # Goiás
    'MT': 'CENTRO-OESTE',  # Mato Grosso
    'MS': 'CENTRO-OESTE',  # Mato Grosso do Sul
    
    # Região Sudeste
    'ES': 'SUDESTE',  # Espírito Santo
    'MG': 'SUDESTE',  # Minas Gerais
    'RJ': 'SUDESTE',  # Rio de Janeiro
    'SP': 'SUDESTE',  # São Paulo
    
    # Região Sul
    'PR': 'SUL',  # Paraná
    'RS': 'SUL',  # Rio Grande do Sul
    'SC': 'SUL',  # Santa Catarina
}

# Mapeamento de nomes completos para siglas (opcional)
STATE_NAMES_TO_SIGLAS = {
    'ACRE': 'AC',
    'ALAGOAS': 'AL',
    'AMAPA': 'AP',
    'AMAZONAS': 'AM',
    'BAHIA': 'BA',
    'CEARA': 'CE',
    'DISTRITO FEDERAL': 'DF',
    'ESPIRITO SANTO': 'ES',
    'GOIAS': 'GO',
    'MARANHAO': 'MA',
    'MATO GROSSO': 'MT',
    'MATO GROSSO DO SUL': 'MS',
    'MINAS GERAIS': 'MG',
    'PARA': 'PA',
    'PARAIBA': 'PB',
    'PARANA': 'PR',
    'PERNAMBUCO': 'PE',
    'PIAUI': 'PI',
    'RIO DE JANEIRO': 'RJ',
    'RIO GRANDE DO NORTE': 'RN',
    'RIO GRANDE DO SUL': 'RS',
    'RONDONIA': 'RO',
    'RORAIMA': 'RR',
    'SANTA CATARINA': 'SC',
    'SAO PAULO': 'SP',
    'SERGIPE': 'SE',
    'TOCANTINS': 'TO'
}

# Informações adicionais sobre as regiões
REGION_INFO = {
    'NORTE': {
        'name': 'Norte',
        'states': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
        'capital': 'Manaus (AM)',
        'area_km2': 3869637,
        'population': 18872349,
        'description': 'Maior região em área, rica em biodiversidade'
    },
    'NORDESTE': {
        'name': 'Nordeste',
        'states': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
        'capital': 'Salvador (BA)',
        'area_km2': 1558196,
        'population': 57901003,
        'description': 'Região com litoral extenso e rica cultura'
    },
    'CENTRO-OESTE': {
        'name': 'Centro-Oeste',
        'states': ['DF', 'GO', 'MT', 'MS'],
        'capital': 'Brasília (DF)',
        'area_km2': 1606371,
        'population': 16438633,
        'description': 'Região do agronegócio e capital federal'
    },
    'SUDESTE': {
        'name': 'Sudeste',
        'states': ['ES', 'MG', 'RJ', 'SP'],
        'capital': 'São Paulo (SP)',
        'area_km2': 924620,
        'population': 89111491,
        'description': 'Região mais populosa e economicamente desenvolvida'
    },
    'SUL': {
        'name': 'Sul',
        'states': ['PR', 'RS', 'SC'],
        'capital': 'Curitiba (PR)',
        'area_km2': 576409,
        'population': 30264579,
        'description': 'Região com forte influência europeia e desenvolvimento industrial'
    }
}

# Mapeamento antigo (mantido para compatibilidade)
REGION_MAPPING = STATE_TO_REGION

def get_region_by_state(sigla: str) -> str:
    """Retorna a região de um estado pela sigla"""
    sigla = sigla.strip().upper()
    return STATE_TO_REGION.get(sigla, 'DESCONHECIDA')

def get_state_name(sigla: str) -> str:
    """Retorna o nome completo do estado pela sigla"""
    # Mapeamento inverso
    sigla_to_name = {v: k for k, v in STATE_NAMES_TO_SIGLAS.items()}
    return sigla_to_name.get(sigla, sigla)

def get_region_info(region: str) -> dict:
    """Retorna informações sobre uma região"""
    region = region.strip().upper()
    return REGION_INFO.get(region, {})

def get_all_regions() -> list:
    """Retorna lista de todas as regiões"""
    return list(REGION_INFO.keys())

def get_states_by_region(region: str) -> list:
    """Retorna lista de estados de uma região"""
    region = region.strip().upper()
    return REGION_INFO.get(region, {}).get('states', [])

def validate_state_sigla(sigla: str) -> bool:
    """Valida se uma sigla de estado é válida"""
    return sigla.strip().upper() in STATE_TO_REGION

def normalize_state_name(state_input: str) -> str:
    """
    Normaliza entrada de estado (pode ser sigla ou nome completo)
    Retorna sigla normalizada
    """
    if not state_input:
        return ''
    
    state_input = state_input.strip().upper()
    
    # Se já é sigla válida
    if state_input in STATE_TO_REGION:
        return state_input
    
    # Se é nome completo
    if state_input in STATE_NAMES_TO_SIGLAS:
        return STATE_NAMES_TO_SIGLAS[state_input]
    
    # Tentar encontrar por similaridade
    for full_name, sigla in STATE_NAMES_TO_SIGLAS.items():
        if state_input in full_name or full_name in state_input:
            return sigla
    
    return state_input  # Retorna original se não encontrar
