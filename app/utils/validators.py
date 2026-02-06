import re
from typing import Optional, Tuple
from datetime import datetime

class DataValidator:
    """Validador de dados da ANP"""
    
    @staticmethod
    def validate_municipio(name: str) -> Tuple[bool, Optional[str]]:
        """Valida nome do município"""
        if not name or not isinstance(name, str):
            return False, "Nome do município inválido"
        
        name = name.strip()
        
        if len(name) < 2:
            return False, "Nome do município muito curto"
        
        if len(name) > 100:
            return False, "Nome do município muito longo"
        
        # Validar caracteres (permitir letras, espaços e hífens)
        if not re.match(r'^[A-ZÀ-Ü\s\-]+$', name, re.IGNORECASE):
            return False, "Nome contém caracteres inválidos"
        
        return True, None
    
    @staticmethod
    def validate_estado(sigla: str) -> Tuple[bool, Optional[str]]:
        """Valida sigla do estado"""
        siglas_validas = {
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        }
        
        if not sigla or not isinstance(sigla, str):
            return False, "Sigla do estado inválida"
        
        sigla = sigla.strip().upper()
        
        if sigla not in siglas_validas:
            return False, f"Sigla do estado inválida: {sigla}"
        
        return True, None
    
    @staticmethod
    def validate_produto(product: str) -> Tuple[bool, Optional[str]]:
        """Valida tipo de produto/combustível"""
        produtos_validos = {
            'GASOLINA', 'GASOLINA COMUM', 'GASOLINA ADITIVADA',
            'DIESEL', 'DIESEL S10', 'DIESEL S500',
            'GNV', 'GAS NATURAL VEICULAR', 'ETANOL', 'ALCOOL'
        }
        
        if not product or not isinstance(product, str):
            return False, "Tipo de produto inválido"
        
        product = product.strip().upper()
        
        if product not in produtos_validos:
            return False, f"Tipo de produto inválido: {product}"
        
        return True, None
    
    @staticmethod
    def validate_preco(preco: float) -> Tuple[bool, Optional[str]]:
        """Valida preço do combustível"""
        if not isinstance(preco, (int, float)):
            return False, "Preço deve ser um número"
        
        if preco <= 0:
            return False, "Preço deve ser maior que zero"
        
        if preco > 100:  # Preço máximo razoável
            return False, f"Preço inválido: R$ {preco:.2f}"
        
        return True, None
    
    @staticmethod
    def validate_numero_postos(numero: int) -> Tuple[bool, Optional[str]]:
        """Valida número de postos pesquisados"""
        if not isinstance(numero, (int, float)):
            return False, "Número de postos deve ser um número"
        
        numero = int(numero)
        
        if numero < 0:
            return False, "Número de postos não pode ser negativo"
        
        if numero > 10000:  # Número máximo razoável
            return False, f"Número de postos inválido: {numero}"
        
        return True, None
    
    @staticmethod
    def validate_data_referencia(data_str: str) -> Tuple[bool, Optional[str]]:
        """Valida data de referência"""
        try:
            if not data_str:
                return False, "Data de referência vazia"
            
            # Tentar diferentes formatos de data
            formats = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y%m%d']
            
            for fmt in formats:
                try:
                    datetime.strptime(data_str, fmt)
                    return True, None
                except ValueError:
                    continue
            
            return False, f"Formato de data inválido: {data_str}"
            
        except Exception as e:
            return False, f"Erro ao validar data: {str(e)}"
    
    @staticmethod
    def validate_consolidated_fuel_type(fuel_type: str) -> Tuple[bool, Optional[str]]:
        """Valida tipo de combustível consolidado"""
        tipos_validos = ['GASOLINA', 'DIESEL', 'DIESEL_S10', 'GNV', 'ETANOL']
        
        if not fuel_type or not isinstance(fuel_type, str):
            return False, "Tipo de combustível inválido"
        
        fuel_type = fuel_type.strip().upper()
        
        if fuel_type not in tipos_validos:
            return False, f"Tipo de combustível inválido: {fuel_type}"
        
        return True, None
    
    @staticmethod
    def validate_region(region: str) -> Tuple[bool, Optional[str]]:
        """Valida região do Brasil"""
        regioes_validas = ['NORTE', 'NORDESTE', 'CENTRO-OESTE', 'SUDESTE', 'SUL']
        
        if not region or not isinstance(region, str):
            return False, "Região inválida"
        
        region = region.strip().upper()
        
        if region not in regioes_validas:
            return False, f"Região inválida: {region}"
        
        return True, None
    
    @staticmethod
    def validate_coordinate(coord: float, coord_type: str = 'latitude') -> Tuple[bool, Optional[str]]:
        """Valida coordenada geográfica"""
        if not isinstance(coord, (int, float)):
            return False, f"{coord_type} deve ser um número"
        
        if coord_type == 'latitude':
            if coord < -90 or coord > 90:
                return False, f"Latitude inválida: {coord}"
        else:  # longitude
            if coord < -180 or coord > 180:
                return False, f"Longitude inválida: {coord}"
        
        return True, None
    
    @staticmethod
    def validate_complete_record(record: dict) -> Tuple[bool, Optional[str], dict]:
        """
        Valida registro completo de dados da ANP
        
        Retorna: (valido, mensagem_erro, registro_corrigido)
        """
        errors = []
        corrected_record = record.copy()
        
        # Validar município
        if 'MUNICIPIO' in record:
            valid, error = DataValidator.validate_municipio(record['MUNICIPIO'])
            if not valid:
                errors.append(f"Município: {error}")
        else:
            errors.append("Campo MUNICIPIO ausente")
        
        # Validar estado
        if 'ESTADO' in record:
            valid, error = DataValidator.validate_estado(record['ESTADO'])
            if not valid:
                errors.append(f"Estado: {error}")
        else:
            errors.append("Campo ESTADO ausente")
        
        # Validar produto
        if 'PRODUTO' in record:
            valid, error = DataValidator.validate_produto(record['PRODUTO'])
            if not valid:
                errors.append(f"Produto: {error}")
        else:
            errors.append("Campo PRODUTO ausente")
        
        # Validar preço
        if 'PRECO_MEDIO_REVENDA' in record:
            try:
                preco = float(record['PRECO_MEDIO_REVENDA'])
                valid, error = DataValidator.validate_preco(preco)
                if not valid:
                    errors.append(f"Preço: {error}")
                else:
                    corrected_record['PRECO_MEDIO_REVENDA'] = preco
            except (ValueError, TypeError):
                errors.append("Preço não é um número válido")
        else:
            errors.append("Campo PRECO_MEDIO_REVENDA ausente")
        
        # Validar número de postos (opcional)
        if 'NUMERO_DE_POSTOS_PESQUISADOS' in record:
            try:
                numero = int(record['NUMERO_DE_POSTOS_PESQUISADOS'])
                valid, error = DataValidator.validate_numero_postos(numero)
                if not valid:
                    errors.append(f"Número de postos: {error}")
                else:
                    corrected_record['NUMERO_DE_POSTOS_PESQUISADOS'] = numero
            except (ValueError, TypeError):
                errors.append("Número de postos não é um número válido")
        
        if errors:
            return False, "; ".join(errors), corrected_record
        
        return True, None, corrected_record
