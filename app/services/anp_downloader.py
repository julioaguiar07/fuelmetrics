import requests
import pandas as pd
import hashlib
import json
from datetime import datetime, timedelta
import os
from pathlib import Path
import logging
from app.config import settings
import traceback

logger = logging.getLogger(__name__)

class ANPDownloader:
    def __init__(self):
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # URL base da ANP
        self.base_url = settings.ANP_BASE_URL
        self.current_year = datetime.now().year
        
        # Configurar sessão com timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'FuelMetrics/1.0 (https://fuelmetrics.com.br; contato@fuelmetrics.com.br)'
        })
        self.timeout = (10, 30)  # (connect timeout, read timeout)
        
    def get_latest_file_url(self):
        """Gera URL do arquivo mais recente baseado no ano atual"""
        # Tentar diferentes formatos de nome de arquivo
        possible_filenames = [
            f"semanal-municipios-{self.current_year}.xlsx",
            f"semanal_municipios_{self.current_year}.xlsx",
            f"municipios-{self.current_year}.xlsx"
        ]
        
        for filename in possible_filenames:
            url = f"{self.base_url}/{filename}"
            logger.debug(f"Testando URL: {url}")
            yield url
    
    def download_file(self, force=False):
        """Baixa arquivo da ANP se necessário"""
        urls = list(self.get_latest_file_url())
        local_path = self.data_dir / f"anp_data_{self.current_year}.xlsx"
        
        # Verificar se já temos arquivo recente
        if not force and self._should_download(local_path):
            logger.info(f"Usando arquivo em cache: {local_path}")
            return local_path
        
        # Tentar cada URL possível
        last_error = None
        for url in urls:
            try:
                logger.info(f"Tentando baixar: {url}")
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                # Verificar se é realmente um arquivo Excel
                if response.headers.get('Content-Type', '').lower() not in [
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'application/vnd.ms-excel',
                    'application/octet-stream'
                ]:
                    logger.warning(f"Conteúdo não parece ser Excel: {url}")
                    continue
                
                # Verificar tamanho mínimo
                content_length = int(response.headers.get('Content-Length', 0))
                if content_length > 0 and content_length < 1024:  # Menor que 1KB
                    logger.warning(f"Arquivo muito pequeno: {content_length} bytes")
                    continue
                
                # Salvar arquivo
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"Arquivo baixado com sucesso: {local_path}")
                logger.info(f"Tamanho: {os.path.getsize(local_path) / 1024 / 1024:.2f} MB")
                
                # Validar que podemos ler o arquivo
                try:
                    test_df = pd.read_excel(local_path, nrows=5)
                    logger.info(f"Arquivo válido, colunas: {list(test_df.columns)}")
                except Exception as e:
                    logger.error(f"Erro ao validar arquivo Excel: {e}")
                    os.remove(local_path)
                    continue
                
                # Salvar metadados
                self._save_metadata(local_path, response.headers)
                
                return local_path
                
            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning(f"Falha ao baixar {url}: {e}")
                continue
            except Exception as e:
                last_error = e
                logger.error(f"Erro inesperado ao baixar {url}: {e}")
                continue
        
        # Se todas as URLs falharem
        logger.error(f"Todas as URLs falharam. Último erro: {last_error}")
        
        if local_path.exists():
            logger.warning(f"Usando versão cacheada anteriormente: {local_path}")
            return local_path
        
        raise Exception(f"Não foi possível baixar arquivo da ANP. Último erro: {last_error}")
    
    def _should_download(self, filepath: Path) -> bool:
        """Verifica se deve baixar novamente"""
        if not filepath.exists():
            return True
        
        try:
            # Verificar idade do arquivo
            file_age = datetime.now() - datetime.fromtimestamp(filepath.stat().st_mtime)
            
            # Verificar metadados
            metadata_path = filepath.with_suffix('.json')
            if metadata_path.exists():
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                # Verificar se temos data de download
                if 'download_date' in metadata:
                    download_date = datetime.fromisoformat(metadata['download_date'])
                    file_age = datetime.now() - download_date
                
                logger.debug(f"Idade do arquivo: {file_age.days} dias")
            
            # Atualizar se for mais velho que o intervalo configurado
            return file_age.days >= settings.ANP_UPDATE_INTERVAL_DAYS
            
        except Exception as e:
            logger.warning(f"Erro ao verificar idade do arquivo: {e}")
            return True  # Em caso de erro, baixar novamente
    
    def _save_metadata(self, filepath: Path, headers: dict):
        """Salva metadados do download"""
        try:
            metadata = {
                'download_date': datetime.now().isoformat(),
                'content_length': headers.get('Content-Length'),
                'last_modified': headers.get('Last-Modified'),
                'etag': headers.get('ETag'),
                'content_type': headers.get('Content-Type'),
                'file_size': os.path.getsize(filepath),
                'file_hash': self._calculate_file_hash(filepath)
            }
            
            metadata_path = filepath.with_suffix('.json')
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Metadados salvos: {metadata_path}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar metadados: {e}")
    
    def _calculate_file_hash(self, filepath: Path) -> str:
        """Calcula hash do arquivo para verificação de integridade"""
        try:
            hasher = hashlib.sha256()
            with open(filepath, 'rb') as f:
                # Ler em chunks para arquivos grandes
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            logger.error(f"Erro ao calcular hash: {e}")
            return ""
    
    def load_data(self):
        """Carrega dados do Excel para DataFrame"""
        filepath = self.download_file()
        
        try:
            logger.info(f"Lendo arquivo Excel: {filepath}")
            
            # Tentar detectar a aba correta
            excel_file = pd.ExcelFile(filepath)
            logger.info(f"Abas disponíveis: {excel_file.sheet_names}")
            
            # Geralmente a primeira aba contém os dados
            sheet_name = excel_file.sheet_names[0]
            logger.info(f"Usando aba: {sheet_name}")
            
            # Ler dados
            # Primeiro, vamos descobrir onde começa os dados
            temp_df = pd.read_excel(filepath, sheet_name=sheet_name, header=None, nrows=20)
            
            # Encontrar a linha que contém "MÊS" (cabeçalho real)
            header_row = None
            for i in range(len(temp_df)):
                row_values = temp_df.iloc[i].astype(str).str.strip().tolist()
                if 'MÊS' in row_values:
                    header_row = i
                    break
            
            if header_row is None:
                # Se não encontrar, tentar encontrar "MUNICÍPIO"
                for i in range(len(temp_df)):
                    row_values = temp_df.iloc[i].astype(str).str.strip().tolist()
                    if 'MUNICÍPIO' in row_values:
                        header_row = i
                        break
            
            if header_row is None:
                header_row = 0  # Fallback: usar primeira linha como cabeçalho
                
            logger.info(f"Cabeçalho encontrado na linha: {header_row}")
            
            # Agora ler os dados a partir da linha do cabeçalho
            df = pd.read_excel(
                filepath,
                sheet_name=sheet_name,
                header=header_row,
                dtype={'PREÇO MÉDIO REVENDA': 'float64'}
            )
            
            # Remover linhas completamente vazias
            df = df.dropna(how='all')
            
            logger.info(f"Dados lidos: {len(df)} linhas, {len(df.columns)} colunas")
            logger.info(f"Colunas originais: {list(df.columns)}")
            
            # Renomear colunas para formato snake_case
            df.columns = [
                col.strip().upper()
                .replace('Ç', 'C')
                .replace('Ã', 'A')
                .replace('Õ', 'O')
                .replace('É', 'E')
                .replace('Á', 'A')
                .replace(' ', '_')
                .replace('-', '_')
                .replace('(', '')
                .replace(')', '')
                for col in df.columns
            ]
            
            # Verificar colunas esperadas
            expected_columns = [
                'MUNICIPIO', 'ESTADO', 'PRODUTO', 
                'PRECO_MEDIO_REVENDA', 'NUMERO_DE_POSTOS_PESQUISADOS'
            ]
            
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Colunas ausentes: {missing_columns}")
                logger.info(f"Colunas disponíveis: {list(df.columns)}")
                
                # Tentar mapear colunas alternativas
                column_mapping = {
                    'MUNICIPIO': ['MUNICIPIO', 'CIDADE', 'MUNICÍPIO'],
                    'ESTADO': ['ESTADO', 'UF', 'SIGLA'],
                    'PRODUTO': ['PRODUTO', 'COMBUSTIVEL', 'COMBUSTÍVEL'],
                    'PRECO_MEDIO_REVENDA': ['PRECO_MEDIO_REVENDA', 'PRECO', 'PREÇO', 'VALOR'],
                    'NUMERO_DE_POSTOS_PESQUISADOS': ['NUMERO_DE_POSTOS_PESQUISADOS', 'POSTOS', 'QTD_POSTOS']
                }
                
                for expected, alternatives in column_mapping.items():
                    if expected not in df.columns:
                        for alt in alternatives:
                            if alt in df.columns:
                                df = df.rename(columns={alt: expected})
                                logger.info(f"Renomeado {alt} para {expected}")
                                break
            
            logger.info(f"Colunas após processamento: {list(df.columns)}")
            logger.info(f"Dados carregados com sucesso: {len(df)} registros")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {e}")
            logger.error(traceback.format_exc())
            
            # Tentar fallback: ler todas as abas
            try:
                logger.info("Tentando fallback: ler todas as abas")
                all_dfs = []
                for sheet in excel_file.sheet_names:
                    try:
                        sheet_df = pd.read_excel(filepath, sheet_name=sheet)
                        all_dfs.append(sheet_df)
                        logger.info(f"Lida aba {sheet}: {len(sheet_df)} registros")
                    except:
                        continue
                
                if all_dfs:
                    df = pd.concat(all_dfs, ignore_index=True)
                    logger.info(f"Dados concatenados: {len(df)} registros")
                    return df
            except:
                pass
            
            raise Exception(f"Não foi possível ler os dados da ANP: {e}")
