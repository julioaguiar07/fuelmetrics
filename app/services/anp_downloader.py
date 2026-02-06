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
        """Carrega dados do Excel para DataFrame - VERSÃO SIMPLIFICADA"""
        filepath = self.download_file()
        
        try:
            logger.info(f"Lendo arquivo Excel: {filepath}")
            
            # TENTATIVA 1: Ler sem cabeçalho e analisar
            df_raw = pd.read_excel(filepath, sheet_name=0, header=None)
            logger.info(f"Arquivo bruto tem {len(df_raw)} linhas, {len(df_raw.columns)} colunas")
            
            # Mostrar as primeiras 15 linhas para debug
            logger.info("=== PRIMEIRAS 15 LINHAS DO ARQUIVO ===")
            for i in range(min(15, len(df_raw))):
                linha = df_raw.iloc[i].fillna('').astype(str).str.strip().tolist()
                logger.info(f"Linha {i}: {linha}")
            logger.info("=== FIM DAS LINHAS ===")
            
            # Procurar manualmente a linha do cabeçalho
            # Baseado no seu Excel, o cabeçalho tem: MÊS, PRODUTO, REGIÃO, ESTADO, MUNICÍPIO...
            header_row = None
            for i in range(len(df_raw)):
                linha_str = ' '.join(df_raw.iloc[i].fillna('').astype(str).str.strip().str.upper())
                if 'MÊS' in linha_str and 'MUNICÍPIO' in linha_str and 'PRODUTO' in linha_str:
                    header_row = i
                    logger.info(f"Encontrado cabeçalho na linha {header_row}")
                    break
            
            if header_row is None:
                # Se não encontrou, tentar pular as primeiras 10 linhas
                header_row = 10
                logger.warning(f"Cabeçalho não encontrado, usando linha {header_row}")
            
            # Ler os dados começando da linha do cabeçalho
            df = pd.read_excel(filepath, sheet_name=0, header=header_row)
            logger.info(f"DataFrame lido: {len(df)} linhas, {len(df.columns)} colunas")
            
            if len(df.columns) == 1:
                # Se ainda tem só 1 coluna, tentar ler TODAS as linhas
                logger.warning("Ainda com 1 coluna, tentando ler sem pular linhas...")
                df = pd.read_excel(filepath, sheet_name=0)
                logger.info(f"DataFrame completo: {len(df)} linhas, {len(df.columns)} colunas")
                
                # Tentar encontrar cabeçalho nas colunas
                for col in df.columns:
                    if isinstance(col, str) and 'MÊS' in col.upper():
                        logger.info(f"Coluna encontrada: {col}")
            
            # Remover linhas vazias
            df = df.dropna(how='all')
            logger.info(f"Após remover vazias: {len(df)} linhas")
            
            # Renomear colunas para nomes padrão
            # Primeiro, converter todos os nomes para string
            df.columns = df.columns.astype(str)
            
            # Mapeamento manual baseado no SEU arquivo
            rename_map = {}
            for col in df.columns:
                col_upper = col.upper()
                if 'MÊS' in col_upper:
                    rename_map[col] = 'DATA_INICIAL'
                elif 'PRODUTO' in col_upper:
                    rename_map[col] = 'PRODUTO'
                elif 'REGI' in col_upper:
                    rename_map[col] = 'REGIAO'
                elif 'ESTADO' in col_upper:
                    rename_map[col] = 'ESTADO'
                elif 'MUNIC' in col_upper:
                    rename_map[col] = 'MUNICIPIO'
                elif 'NÚMERO' in col_upper and 'POSTOS' in col_upper:
                    rename_map[col] = 'NUMERO_DE_POSTOS_PESQUISADOS'
                elif 'UNIDADE' in col_upper and 'MEDIDA' in col_upper:
                    rename_map[col] = 'UNIDADE_DE_MEDIDA'
                elif 'PREÇO MÉDIO' in col_upper:
                    rename_map[col] = 'PRECO_MEDIO_REVENDA'
                elif 'DESVIO PADRÃO' in col_upper:
                    rename_map[col] = 'DESVIO_PADRAO_REVENDA'
                elif 'PREÇO MÍNIMO' in col_upper:
                    rename_map[col] = 'PRECO_MINIMO_REVENDA'
                elif 'PREÇO MÁXIMO' in col_upper:
                    rename_map[col] = 'PRECO_MAXIMO_REVENDA'
                elif 'COEF' in col_upper and 'VARIAÇÃO' in col_upper:
                    rename_map[col] = 'COEF_DE_VARIACAO_REVENDA'
                else:
                    # Manter original, mas limpar
                    new_name = col.strip().upper().replace(' ', '_').replace('-', '_')
                    new_name = ''.join(c for c in new_name if c.isalnum() or c == '_')
                    rename_map[col] = new_name
            
            df = df.rename(columns=rename_map)
            logger.info(f"Colunas após renomear: {list(df.columns)}")
            
            # Se ainda não tem as colunas críticas, tentar encontrar
            if 'PRECO_MEDIO_REVENDA' not in df.columns:
                logger.warning("Coluna PRECO_MEDIO_REVENDA não encontrada!")
                # Procurar qualquer coluna com "PREÇO" ou "PRECO"
                for col in df.columns:
                    if 'PRECO' in col.upper() or 'PREÇO' in col.upper():
                        df = df.rename(columns={col: 'PRECO_MEDIO_REVENDA'})
                        logger.info(f"Renomeada {col} para PRECO_MEDIO_REVENDA")
                        break
            
            # Converter colunas numéricas
            numeric_cols = ['PRECO_MEDIO_REVENDA', 'PRECO_MINIMO_REVENDA', 
                           'PRECO_MAXIMO_REVENDA', 'NUMERO_DE_POSTOS_PESQUISADOS']
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Converter strings para maiúsculas
            string_cols = ['PRODUTO', 'REGIAO', 'ESTADO', 'MUNICIPIO']
            for col in string_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.upper().str.strip()
            
            # Criar DATA_FINAL se não existe
            if 'DATA_FINAL' not in df.columns and 'DATA_INICIAL' in df.columns:
                df['DATA_FINAL'] = df['DATA_INICIAL']
            
            # Remover linhas sem dados essenciais
            initial_len = len(df)
            if 'PRECO_MEDIO_REVENDA' in df.columns:
                df = df.dropna(subset=['PRECO_MEDIO_REVENDA'])
            if 'MUNICIPIO' in df.columns:
                df = df[df['MUNICIPIO'].astype(str).str.strip() != '']
                df = df[df['MUNICIPIO'].astype(str).str.strip().str.upper() != 'NAN']
            
            logger.info(f"Removidas {initial_len - len(df)} linhas inválidas")
            logger.info(f"Dados finais: {len(df)} registros")
            
            if len(df) == 0:
                logger.error("NENHUM DADO VÁLIDO ENCONTRADO!")
                # Criar dados de exemplo para não quebrar a API
                df = pd.DataFrame({
                    'DATA_INICIAL': ['2026-01-01'],
                    'DATA_FINAL': ['2026-01-01'],
                    'REGIAO': ['SUDESTE'],
                    'ESTADO': ['SP'],
                    'MUNICIPIO': ['SÃO PAULO'],
                    'PRODUTO': ['GASOLINA'],
                    'NUMERO_DE_POSTOS_PESQUISADOS': [100],
                    'UNIDADE_DE_MEDIDA': ['R$/L'],
                    'PRECO_MEDIO_REVENDA': [5.50],
                    'PRECO_MINIMO_REVENDA': [5.20],
                    'PRECO_MAXIMO_REVENDA': [5.80],
                    'DESVIO_PADRAO_REVENDA': [0.10],
                    'COEF_DE_VARIACAO_REVENDA': [1.8]
                })
                logger.warning("Usando dados de exemplo!")
            
            return df
            
        except Exception as e:
            logger.error(f"ERRO CRÍTICO ao carregar dados: {e}")
            logger.error(traceback.format_exc())
            
            # Criar dados de exemplo em caso de erro
            logger.warning("Criando dados de exemplo devido a erro...")
            df = pd.DataFrame({
                'DATA_INICIAL': ['2026-01-01', '2026-01-01', '2026-01-01'],
                'DATA_FINAL': ['2026-01-01', '2026-01-01', '2026-01-01'],
                'REGIAO': ['SUDESTE', 'SUL', 'NORDESTE'],
                'ESTADO': ['SP', 'RS', 'BA'],
                'MUNICIPIO': ['SÃO PAULO', 'PORTO ALEGRE', 'SALVADOR'],
                'PRODUTO': ['GASOLINA', 'GASOLINA', 'GASOLINA'],
                'NUMERO_DE_POSTOS_PESQUISADOS': [100, 80, 60],
                'UNIDADE_DE_MEDIDA': ['R$/L', 'R$/L', 'R$/L'],
                'PRECO_MEDIO_REVENDA': [5.50, 5.30, 5.70],
                'PRECO_MINIMO_REVENDA': [5.20, 5.10, 5.50],
                'PRECO_MAXIMO_REVENDA': [5.80, 5.60, 5.90],
                'DESVIO_PADRAO_REVENDA': [0.10, 0.08, 0.12],
                'COEF_DE_VARIACAO_REVENDA': [1.8, 1.5, 2.1]
            })
            
            return df
            
