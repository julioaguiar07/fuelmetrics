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

    
# NOVO MÉTODO load_data - substitua o método atual pelo abaixo:

def load_data(self):
    """Carrega dados do Excel para DataFrame - VERSÃO CORRIGIDA"""
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
        
        # Procurar a linha que tem "MÊS" - que é o cabeçalho no SEU arquivo
        header_row = None
        for i in range(min(30, len(df_all))):
            linha_str = ' '.join(df_all.iloc[i].fillna('').astype(str).str.strip().str.upper())
            if 'MÊS' in linha_str or 'MES' in linha_str:
                header_row = i
                logger.info(f"Cabeçalho encontrado na linha {header_row}: {linha_str[:200]}...")
                break
        
        if header_row is None:
            # Tentativa alternativa
            for i in range(min(30, len(df_all))):
                linha_str = ' '.join(df_all.iloc[i].fillna('').astype(str).str.strip().str.upper())
                if 'PRODUTO' in linha_str and 'REGIÃO' in linha_str:
                    header_row = i
                    logger.info(f"Cabeçalho alternativo na linha {header_row}")
                    break
        
        if header_row is None:
            header_row = 0
            logger.warning(f"Cabeçalho não encontrado, usando linha 0")
        
        # MÉTODO 2: Ler com o cabeçalho encontrado
        logger.info(f"\nMétodo 2: Lendo com header={header_row}...")
        df = pd.read_excel(filepath, sheet_name=0, header=header_row)
        
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"Colunas: {list(df.columns)}")
        
        # REMOVER: Mostrar estrutura - isso já está no seu log original
        
        # Remover linhas completamente vazias
        df = df.dropna(how='all')
        logger.info(f"Após remover vazias: {len(df)} linhas")
        
        # **CRÍTICO: Normalizar nomes das colunas BASEADO NO SEU ARQUIVO**
        # Seu arquivo tem: MÊS, PRODUTO, REGIÃO, ESTADO, MUNICÍPIO, NÚMERO DE POSTOS PESQUISADOS, etc.
        
        # Converter todas as colunas para string e remover espaços
        df.columns = [str(col).strip() for col in df.columns]
        
        # **Mapeamento ESPECÍFICO para sua estrutura**
        column_mapping = {}
        for col in df.columns:
            col_str = str(col).upper().strip()
            
            # Mapeamento exato baseado no seu exemplo
            if col_str == 'MÊS' or col_str == 'MES':
                column_mapping[col] = 'DATA_INICIAL'
            elif 'PRODUTO' in col_str:
                column_mapping[col] = 'PRODUTO'
            elif 'REGIÃO' in col_str or 'REGIAO' in col_str:
                column_mapping[col] = 'REGIAO'
            elif 'ESTADO' in col_str:
                column_mapping[col] = 'ESTADO'
            elif 'MUNICÍPIO' in col_str or 'MUNICIPIO' in col_str:
                column_mapping[col] = 'MUNICIPIO'
            elif 'NÚMERO' in col_str and 'POSTOS' in col_str:
                column_mapping[col] = 'NUMERO_DE_POSTOS_PESQUISADOS'
            elif 'UNIDADE' in col_str and 'MEDIDA' in col_str:
                column_mapping[col] = 'UNIDADE_DE_MEDIDA'
            elif 'PREÇO MÉDIO REVENDA' in col_str or 'PRECO MEDIO REVENDA' in col_str:
                column_mapping[col] = 'PRECO_MEDIO_REVENDA'
            elif 'DESVIO PADRÃO REVENDA' in col_str or 'DESVIO PADRAO REVENDA' in col_str:
                column_mapping[col] = 'DESVIO_PADRAO_REVENDA'
            elif 'PREÇO MÍNIMO REVENDA' in col_str or 'PRECO MINIMO REVENDA' in col_str:
                column_mapping[col] = 'PRECO_MINIMO_REVENDA'
            elif 'PREÇO MÁXIMO REVENDA' in col_str or 'PRECO MAXIMO REVENDA' in col_str:
                column_mapping[col] = 'PRECO_MAXIMO_REVENDA'
            elif 'COEF' in col_str and 'VARIAÇÃO' in col_str:
                column_mapping[col] = 'COEF_DE_VARIACAO_REVENDA'
            else:
                # Manter original mas limpar
                new_name = col_str.replace(' ', '_').replace('Ç', 'C').replace('Ã', 'A')
                column_mapping[col] = new_name
        
        df = df.rename(columns=column_mapping)
        logger.info(f"Colunas após mapeamento: {list(df.columns)}")
        
        # **CRÍTICO: Processar a coluna MÊS (jan/26)**
        if 'DATA_INICIAL' in df.columns:
            # Converter formato "jan/26" para data
            def parse_month_year(value):
                try:
                    if pd.isna(value):
                        return pd.NaT
                    
                    value_str = str(value).lower().strip()
                    
                    # Se já é uma data, retornar
                    if isinstance(value, (datetime, pd.Timestamp)):
                        return value
                    
                    # Formato "jan/26"
                    if '/' in value_str:
                        month_str, year_str = value_str.split('/')
                        
                        # Mapear mês
                        month_map = {
                            'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
                            'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12
                        }
                        
                        month = month_map.get(month_str[:3].lower(), 1)
                        year = int('20' + year_str) if len(year_str) == 2 else int(year_str)
                        
                        # Criar data como primeiro dia do mês
                        return datetime(year, month, 1)
                    
                    return pd.NaT
                except:
                    return pd.NaT
            
            df['DATA_INICIAL'] = df['DATA_INICIAL'].apply(parse_month_year)
            # Criar DATA_FINAL como último dia do mês
            df['DATA_FINAL'] = df['DATA_INICIAL'] + pd.offsets.MonthEnd(0)
        
        # **CRÍTICO: Garantir que PRODUTO está em maiúsculas**
        if 'PRODUTO' in df.columns:
            df['PRODUTO'] = df['PRODUTO'].astype(str).str.upper().str.strip()
            logger.info(f"Produtos únicos encontrados: {df['PRODUTO'].unique()[:10]}")
        
        # **CRÍTICO: Converter preços (notar que no Brasil usa vírgula como decimal)**
        if 'PRECO_MEDIO_REVENDA' in df.columns:
            # Primeiro tentar converter diretamente
            try:
                df['PRECO_MEDIO_REVENDA'] = pd.to_numeric(df['PRECO_MEDIO_REVENDA'], errors='coerce')
            except:
                # Se falhar, pode ser string com vírgula
                df['PRECO_MEDIO_REVENDA'] = df['PRECO_MEDIO_REVENDA'].astype(str).str.replace(',', '.').astype(float)
        
        # Converter outras colunas numéricas
        numeric_columns = ['NUMERO_DE_POSTOS_PESQUISADOS', 'PRECO_MINIMO_REVENDA', 
                          'PRECO_MAXIMO_REVENDA', 'DESVIO_PADRAO_REVENDA', 
                          'COEF_DE_VARIACAO_REVENDA']
        
        for col in numeric_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
                except:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Converter strings para maiúsculas
        for col in ['PRODUTO', 'ESTADO', 'MUNICIPIO', 'REGIAO']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper().str.strip()
        
        # **REMOVER dados de exemplo - NÃO PRECISAMOS DELES**
        # Se tem menos de 100 registros, algo está errado
        if len(df) < 100:
            logger.error(f"POUCOS DADOS: apenas {len(df)} registros")
            # Mostrar primeiros registros para debug
            logger.info(f"Primeiros registros: {df[['PRODUTO', 'MUNICIPIO', 'PRECO_MEDIO_REVENDA']].head(10).to_dict('records')}")
        
        logger.info(f"Dados finais: {len(df)} registros")
        logger.info(f"Produtos encontrados: {df['PRODUTO'].unique() if 'PRODUTO' in df.columns else []}")
        
        logger.info("=" * 60)
        logger.info("FIM DO PROCESSAMENTO")
        logger.info("=" * 60)
        
        return df
        
    except Exception as e:
        logger.error(f"ERRO CRÍTICO no load_data: {e}", exc_info=True)
        # NÃO criar dados de exemplo - deixar falhar para ver o erro
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
            
