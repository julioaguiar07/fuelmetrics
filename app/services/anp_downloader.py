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
        """Carrega dados do Excel para DataFrame - VERSÃO FINAL CORRIGIDA"""
        filepath = self.download_file()
        
        try:
            logger.info("=" * 60)
            logger.info("INÍCIO DO PROCESSAMENTO DO EXCEL")
            logger.info("=" * 60)
            
            # MÉTODO 1: Ler TUDO sem cabeçalho para debug
            logger.info("Método 1: Lendo tudo sem cabeçalho...")
            df_all = pd.read_excel(filepath, sheet_name=0, header=None)
            logger.info(f"Total de linhas brutas: {len(df_all)}")
            logger.info(f"Total de colunas brutas: {len(df_all.columns)}")
            
            # Procurar a linha que tem "DATA INICIAL" - que é o cabeçalho real
            header_row = None
            for i in range(min(20, len(df_all))):
                linha_str = ' '.join(df_all.iloc[i].fillna('').astype(str).str.strip().str.upper())
                if 'DATA INICIAL' in linha_str or 'DATA_INICIAL' in linha_str:
                    header_row = i
                    logger.info(f"Cabeçalho encontrado na linha {header_row}: {linha_str[:200]}...")
                    break
            
            if header_row is None:
                # Tentativa alternativa - procurar por outras colunas chave
                for i in range(min(20, len(df_all))):
                    linha_str = ' '.join(df_all.iloc[i].fillna('').astype(str).str.strip().str.upper())
                    matches = 0
                    if 'MUNICÍPIO' in linha_str or 'MUNICIPIO' in linha_str:
                        matches += 1
                    if 'PRODUTO' in linha_str:
                        matches += 1
                    if 'ESTADO' in linha_str:
                        matches += 1
                    if matches >= 2:
                        header_row = i
                        logger.info(f"Cabeçalho alternativo na linha {header_row} ({matches} matches)")
                        break
            
            if header_row is None:
                # Última tentativa: pular linhas baseado na estrutura
                for i in range(5, 15):
                    linha = df_all.iloc[i].fillna('').astype(str).str.strip().tolist()
                    if len(linha) >= 10 and any('DIESEL' in cell.upper() for cell in linha):
                        header_row = 10  # Assume que o cabeçalho está na linha 10
                        logger.info(f"Usando header padrão na linha {header_row}")
                        break
            
            if header_row is None:
                header_row = 10  # Default baseado na estrutura comum
                logger.warning(f"Cabeçalho não encontrado, usando linha {header_row}")
            
            # MÉTODO 2: Ler com o cabeçalho encontrado
            logger.info(f"\nMétodo 2: Lendo com header={header_row}...")
            try:
                df = pd.read_excel(filepath, sheet_name=0, header=header_row)
            except Exception as e:
                logger.error(f"Erro ao ler Excel com header={header_row}: {e}")
                # Tentar ler sem header e processar manualmente
                df = pd.read_excel(filepath, sheet_name=0)
                # Remover as primeiras linhas manualmente
                df = df.iloc[header_row:]
                df.columns = df.iloc[0]  # Primeira linha como header
                df = df.iloc[1:]
            
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Colunas originais: {list(df.columns)}")
            
            # Remover linhas completamente vazias
            df = df.dropna(how='all')
            logger.info(f"Após remover vazias: {len(df)} linhas")
            
            # **CRÍTICO: Normalizar nomes das colunas para a estrutura REAL**
            # Seu arquivo real tem: DATA INICIAL, DATA FINAL, REGIÃO, ESTADO, MUNICÍPIO, PRODUTO, etc.
            
            # Converter todas as colunas para string e remover espaços
            df.columns = [str(col).strip() for col in df.columns]
            
            # **Mapeamento para estrutura SEMANAL**
            column_mapping = {}
            for col in df.columns:
                col_str = str(col).upper().strip()
                
                # Mapeamento baseado na estrutura REAL
                if 'DATA INICIAL' in col_str or 'DATA_INICIAL' in col_str:
                    column_mapping[col] = 'DATA_INICIAL'
                elif 'DATA FINAL' in col_str or 'DATA_FINAL' in col_str:
                    column_mapping[col] = 'DATA_FINAL'
                elif 'REGIÃO' in col_str or 'REGIAO' in col_str:
                    column_mapping[col] = 'REGIAO'
                elif 'ESTADO' in col_str:
                    column_mapping[col] = 'ESTADO'
                elif 'MUNICÍPIO' in col_str or 'MUNICIPIO' in col_str:
                    column_mapping[col] = 'MUNICIPIO'
                elif 'PRODUTO' in col_str:
                    column_mapping[col] = 'PRODUTO'
                elif 'NÚMERO' in col_str and 'POSTOS' in col_str:
                    column_mapping[col] = 'NUMERO_DE_POSTOS_PESQUISADOS'
                elif 'UNIDADE' in col_str and 'MEDIDA' in col_str:
                    column_mapping[col] = 'UNIDADE_DE_MEDIDA'
                elif 'PREÇO MÉDIO' in col_str or 'PRECO MEDIO' in col_str:
                    if 'REVENDA' in col_str:
                        column_mapping[col] = 'PRECO_MEDIO_REVENDA'
                    else:
                        column_mapping[col] = 'PRECO_MEDIO_REVENDA'  # Default
                elif 'DESVIO' in col_str and 'PADRÃO' in col_str:
                    column_mapping[col] = 'DESVIO_PADRAO_REVENDA'
                elif 'PREÇO MÍNIMO' in col_str or 'PRECO MINIMO' in col_str:
                    column_mapping[col] = 'PRECO_MINIMO_REVENDA'
                elif 'PREÇO MÁXIMO' in col_str or 'PRECO MAXIMO' in col_str:
                    column_mapping[col] = 'PRECO_MAXIMO_REVENDA'
                elif 'COEF' in col_str and 'VARIAÇÃO' in col_str:
                    column_mapping[col] = 'COEF_DE_VARIACAO_REVENDA'
                elif 'MARGEM' in col_str and 'MÉDIA' in col_str:
                    column_mapping[col] = 'MARGEM_MEDIA_REVENDA'
                else:
                    # Manter original mas limpar
                    new_name = col_str.replace(' ', '_').replace('Ç', 'C').replace('Ã', 'A')
                    new_name = new_name.replace('Á', 'A').replace('É', 'E').replace('Í', 'I')
                    new_name = new_name.replace('Ó', 'O').replace('Ú', 'U').replace('Ô', 'O')
                    new_name = new_name.replace('Ê', 'E').replace('Â', 'A')
                    column_mapping[col] = new_name
            
            df = df.rename(columns=column_mapping)
            logger.info(f"Colunas após mapeamento: {list(df.columns)}")
            
            # **CRÍTICO: Processar datas**
            date_columns = ['DATA_INICIAL', 'DATA_FINAL']
            for col in date_columns:
                if col in df.columns:
                    try:
                        # Tentar converter para datetime
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except Exception as e:
                        logger.warning(f"Erro ao converter {col}: {e}")
                        # Tentar formato específico dd/mm/yyyy
                        try:
                            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
                        except:
                            logger.error(f"Não foi possível converter {col}")
            
            # **CRÍTICO: Garantir que PRODUTO está em maiúsculas e limpo**
            if 'PRODUTO' in df.columns:
                df['PRODUTO'] = df['PRODUTO'].astype(str).str.upper().str.strip()
                # Mostrar produtos únicos para debug
                produtos_unicos = df['PRODUTO'].unique()
                logger.info(f"Produtos únicos encontrados ({len(produtos_unicos)}): {produtos_unicos[:20]}")
                
                # Verificar se tem diesel
                tem_diesel = any('DIESEL' in str(p) for p in produtos_unicos)
                logger.info(f"Contém DIESEL? {tem_diesel}")
                
                # Verificar se tem diesel S10
                tem_diesel_s10 = any('S10' in str(p) for p in produtos_unicos)
                logger.info(f"Contém DIESEL S10? {tem_diesel_s10}")
            
            # **CRÍTICO: Converter preços (Brasil usa vírgula como decimal)**
            price_columns = ['PRECO_MEDIO_REVENDA', 'PRECO_MINIMO_REVENDA', 
                            'PRECO_MAXIMO_REVENDA', 'DESVIO_PADRAO_REVENDA',
                            'COEF_DE_VARIACAO_REVENDA']
            
            for col in price_columns:
                if col in df.columns:
                    try:
                        # Primeiro tentar converter diretamente
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except:
                        # Se falhar, pode ser string com vírgula como decimal
                        try:
                            df[col] = df[col].astype(str).str.replace(',', '.').astype(float)
                        except:
                            logger.warning(f"Não foi possível converter {col}")
                            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Converter número de postos para inteiro
            if 'NUMERO_DE_POSTOS_PESQUISADOS' in df.columns:
                df['NUMERO_DE_POSTOS_PESQUISADOS'] = pd.to_numeric(df['NUMERO_DE_POSTOS_PESQUISADOS'], errors='coerce').fillna(0).astype(int)
            
            # Remover linhas sem preço
            initial_count = len(df)
            if 'PRECO_MEDIO_REVENDA' in df.columns:
                df = df.dropna(subset=['PRECO_MEDIO_REVENDA'])
                df = df[df['PRECO_MEDIO_REVENDA'] > 0]
            if 'PRODUTO' in df.columns:
                df = df[~df['PRODUTO'].isin(['', 'NAN', 'NONE', 'NULL'])]
            
            removed_count = initial_count - len(df)
            if removed_count > 0:
                logger.info(f"Removidas {removed_count} linhas inválidas")
            
            # Converter outras strings para maiúsculas
            for col in ['ESTADO', 'MUNICIPIO', 'REGIAO']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.upper().str.strip()
            
            # **IMPORTANTE: Verificar se temos dados**
            logger.info(f"\n=== RESUMO FINAL ===")
            logger.info(f"Total de registros: {len(df)}")
            
            if 'PRODUTO' in df.columns and len(df) > 0:
                produtos_finais = df['PRODUTO'].unique()
                logger.info(f"Produtos no dataset final ({len(produtos_finais)}):")
                for produto in produtos_finais:
                    count = len(df[df['PRODUTO'] == produto])
                    logger.info(f"  - {produto}: {count} registros")
            
            # Verificar colunas essenciais
            essential_cols = ['PRODUTO', 'MUNICIPIO', 'ESTADO', 'PRECO_MEDIO_REVENDA']
            missing_cols = [col for col in essential_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Colunas essenciais faltando: {missing_cols}")
                logger.info(f"Colunas disponíveis: {list(df.columns)}")
            
            logger.info("=" * 60)
            logger.info("FIM DO PROCESSAMENTO")
            logger.info("=" * 60)
            
            return df
            
        except Exception as e:
            logger.error(f"ERRO CRÍTICO no load_data: {e}", exc_info=True)
            # NÃO criar dados de exemplo - levantar erro para debug
            raise
    
    def _create_sample_data(self):
        """Cria dados de exemplo quando não consegue ler o arquivo"""
        import pandas as pd
        import numpy as np
        from datetime import datetime
        
        logger.warning("CRIANDO DADOS DE EXEMPLO")
        
        # Criar dados realistas
        estados = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'PE', 'CE', 'DF']
        municipios = ['SÃO PAULO', 'RIO DE JANEIRO', 'BELO HORIZONTE', 'PORTO ALEGRE', 
                     'CURITIBA', 'FLORIANÓPOLIS', 'SALVADOR', 'RECIFE', 'FORTALEZA', 'BRASÍLIA']
        produtos = ['GASOLINA', 'GASOLINA ADITIVADA', 'ETANOL', 'DIESEL', 'GNV']
        regioes = ['SUDESTE', 'SUL', 'NORDESTE', 'CENTRO-OESTE', 'NORTE']
        
        data = []
        for i in range(100):
            estado = np.random.choice(estados)
            regiao = 'SUDESTE' if estado in ['SP', 'RJ', 'MG'] else \
                    'SUL' if estado in ['RS', 'PR', 'SC'] else \
                    'NORDESTE' if estado in ['BA', 'PE', 'CE'] else 'CENTRO-OESTE'
            
            data.append({
                'DATA_INICIAL': '2026-01-01',
                'DATA_FINAL': '2026-01-01',
                'REGIAO': regiao,
                'ESTADO': estado,
                'MUNICIPIO': np.random.choice(municipios),
                'PRODUTO': np.random.choice(produtos),
                'NUMERO_DE_POSTOS_PESQUISADOS': np.random.randint(10, 100),
                'UNIDADE_DE_MEDIDA': 'R$/L',
                'PRECO_MEDIO_REVENDA': round(np.random.uniform(4.5, 6.5), 2),
                'PRECO_MINIMO_REVENDA': round(np.random.uniform(4.0, 5.5), 2),
                'PRECO_MAXIMO_REVENDA': round(np.random.uniform(5.5, 7.0), 2),
                'DESVIO_PADRAO_REVENDA': round(np.random.uniform(0.05, 0.20), 3),
                'COEF_DE_VARIACAO_REVENDA': round(np.random.uniform(1.0, 3.0), 1)
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Dados de exemplo criados: {len(df)} registros")
        return df
            
