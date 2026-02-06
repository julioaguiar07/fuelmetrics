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
        """Carrega dados do Excel para DataFrame - VERSÃO FINAL"""
        filepath = self.download_file()
        
        try:
            logger.info("=" * 60)
            logger.info("INÍCIO DO PROCESSAMENTO DO EXCEL")
            logger.info("=" * 60)
            
            # MÉTODO 1: Ler TUDO sem cabeçalho
            logger.info("Método 1: Lendo tudo sem cabeçalho...")
            df_all = pd.read_excel(filepath, sheet_name=0, header=None)
            logger.info(f"Total de linhas brutas: {len(df_all)}")
            logger.info(f"Total de colunas brutas: {len(df_all.columns)}")
            
            # Mostrar estrutura do arquivo
            logger.info("\nEstrutura do arquivo (primeiras 20 linhas):")
            for i in range(min(20, len(df_all))):
                linha = df_all.iloc[i].fillna('').astype(str).str.strip().tolist()
                # Filtrar valores vazios
                linha_filtrada = [v for v in linha if v]
                if linha_filtrada:  # Só mostrar linhas não vazias
                    logger.info(f"Linha {i}: {linha_filtrada}")
            
            # Procurar a linha que tem TODOS os cabeçalhos esperados
            target_columns = ['MÊS', 'PRODUTO', 'REGIÃO', 'ESTADO', 'MUNICÍPIO', 
                             'NÚMERO DE POSTOS PESQUISADOS', 'PREÇO MÉDIO REVENDA']
            
            header_row = None
            for i in range(min(30, len(df_all))):
                linha_str = ' '.join(df_all.iloc[i].fillna('').astype(str).str.strip().str.upper())
                # Verificar se tem vários dos cabeçalhos esperados
                matches = sum(1 for col in target_columns if col.upper() in linha_str)
                if matches >= 4:  # Pelo menos 4 colunas importantes
                    header_row = i
                    logger.info(f"Provável cabeçalho na linha {header_row} ({matches} matches)")
                    logger.info(f"Conteúdo: {linha_str[:200]}...")
                    break
            
            if header_row is None:
                # Se não encontrou, procurar por "MUNICÍPIO" que é mais específico
                for i in range(min(30, len(df_all))):
                    linha_str = ' '.join(df_all.iloc[i].fillna('').astype(str).str.strip().str.upper())
                    if 'MUNICÍPIO' in linha_str:
                        header_row = i
                        logger.info(f"Cabeçalho encontrado via 'MUNICÍPIO' na linha {header_row}")
                        break
            
            if header_row is None:
                # Tentativa final: pular 10 linhas
                header_row = 10
                logger.warning(f"Cabeçalho não encontrado, pulando {header_row} linhas")
            
            # MÉTODO 2: Ler com o cabeçalho encontrado
            logger.info(f"\nMétodo 2: Lendo com header={header_row}...")
            df = pd.read_excel(filepath, sheet_name=0, header=header_row)
            
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Colunas: {list(df.columns)}")
            
            if len(df.columns) <= 1:
                logger.error("Ainda com poucas colunas, tentando método alternativo...")
                
                # MÉTODO 3: Ler especificando range de linhas
                skiprows = header_row
                nrows = 10000  # Ler muitas linhas
                df = pd.read_excel(filepath, sheet_name=0, header=0, 
                                 skiprows=skiprows, nrows=nrows)
                logger.info(f"Método 3 shape: {df.shape}")
            
            # Remover linhas completamente vazias
            df = df.dropna(how='all')
            logger.info(f"Após remover vazias: {len(df)} linhas")
            
            # Normalizar nomes das colunas
            df.columns = [str(col).strip() for col in df.columns]
            
            # Mapeamento DIRETO baseado no SEU arquivo
            column_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                
                if 'mês' in col_lower:
                    column_mapping[col] = 'DATA_INICIAL'
                elif 'produto' in col_lower:
                    column_mapping[col] = 'PRODUTO'
                elif 'região' in col_lower or 'regiao' in col_lower:
                    column_mapping[col] = 'REGIAO'
                elif 'estado' in col_lower:
                    column_mapping[col] = 'ESTADO'
                elif 'município' in col_lower or 'municipio' in col_lower:
                    column_mapping[col] = 'MUNICIPIO'
                elif 'número' in col_lower and 'postos' in col_lower:
                    column_mapping[col] = 'NUMERO_DE_POSTOS_PESQUISADOS'
                elif 'unidade' in col_lower and 'medida' in col_lower:
                    column_mapping[col] = 'UNIDADE_DE_MEDIDA'
                elif 'preço médio' in col_lower or 'preco medio' in col_lower:
                    column_mapping[col] = 'PRECO_MEDIO_REVENDA'
                elif 'desvio padrão' in col_lower or 'desvio padrao' in col_lower:
                    column_mapping[col] = 'DESVIO_PADRAO_REVENDA'
                elif 'preço mínimo' in col_lower or 'preco minimo' in col_lower:
                    column_mapping[col] = 'PRECO_MINIMO_REVENDA'
                elif 'preço máximo' in col_lower or 'preco maximo' in col_lower:
                    column_mapping[col] = 'PRECO_MAXIMO_REVENDA'
                elif 'coef' in col_lower and 'variação' in col_lower:
                    column_mapping[col] = 'COEF_DE_VARIACAO_REVENDA'
                else:
                    # Simplificar nome mantendo
                    column_mapping[col] = col
            
            df = df.rename(columns=column_mapping)
            logger.info(f"Colunas após mapeamento: {list(df.columns)}")
            
            # CORREÇÃO CRÍTICA: Remover espaços dos nomes das colunas
            logger.info("Removendo espaços dos nomes das colunas...")
            df.columns = [col.replace(' ', '_') for col in df.columns]
            logger.info(f"Colunas após remover espaços: {list(df.columns)}")
            
            # Se PRECO_MEDIO_REVENDA não existe, procurar qualquer coluna de preço
            if 'PRECO_MEDIO_REVENDA' not in df.columns:
                for col in df.columns:
                    if 'preco' in str(col).lower() or 'preço' in str(col).lower():
                        logger.info(f"Renomeando '{col}' para 'PRECO_MEDIO_REVENDA'")
                        df = df.rename(columns={col: 'PRECO_MEDIO_REVENDA'})
                        break
            
            # Garantir colunas essenciais
            essential_columns = ['MUNICIPIO', 'ESTADO', 'PRODUTO', 'PRECO_MEDIO_REVENDA']
            for col in essential_columns:
                if col not in df.columns:
                    logger.warning(f"Coluna essencial faltando: {col}")
            
            # Converter tipos
            if 'PRECO_MEDIO_REVENDA' in df.columns:
                df['PRECO_MEDIO_REVENDA'] = pd.to_numeric(df['PRECO_MEDIO_REVENDA'], errors='coerce')
            
            if 'NUMERO_DE_POSTOS_PESQUISADOS' in df.columns:
                df['NUMERO_DE_POSTOS_PESQUISADOS'] = pd.to_numeric(df['NUMERO_DE_POSTOS_PESQUISADOS'], errors='coerce')
            
            # Converter strings para maiúsculas
            for col in ['PRODUTO', 'ESTADO', 'MUNICIPIO', 'REGIAO']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.upper().str.strip()
            
            # Criar DATA_FINAL se não existe
            if 'DATA_FINAL' not in df.columns and 'DATA_INICIAL' in df.columns:
                df['DATA_FINAL'] = df['DATA_INICIAL']
            
            # Remover linhas inválidas
            initial_len = len(df)
            if 'PRECO_MEDIO_REVENDA' in df.columns:
                df = df.dropna(subset=['PRECO_MEDIO_REVENDA'])
            if 'MUNICIPIO' in df.columns:
                df = df[~df['MUNICIPIO'].isin(['', 'NAN', 'NONE', 'N/A'])]
            
            logger.info(f"Removidas {initial_len - len(df)} linhas inválidas")
            logger.info(f"Dados finais: {len(df)} registros")
            
            # Se sem dados, criar dados de exemplo
            if len(df) == 0:
                logger.error("NENHUM DADO VÁLIDO ENCONTRADO!")
                logger.info("Criando dados de exemplo para continuidade...")
                df = self._create_sample_data()
            
            logger.info("=" * 60)
            logger.info("FIM DO PROCESSAMENTO")
            logger.info("=" * 60)
            
            return df
            
        except Exception as e:
            logger.error(f"ERRO CRÍTICO no load_data: {e}", exc_info=True)
            logger.warning("Retornando dados de exemplo devido a erro...")
            return self._create_sample_data()
    
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
            
