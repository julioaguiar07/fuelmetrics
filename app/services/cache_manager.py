import pickle
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Any, Optional, Dict
from app.config import settings

logger = logging.getLogger(__name__)

class CacheManager:
    """Gerenciador de cache para dados da ANP"""
    
    def __init__(self):
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        
        # Cache em memória
        self.memory_cache: Dict[str, Dict] = {}
        self.cache_ttl = settings.CACHE_TTL
        
        # Metadados do cache
        self.metadata_file = self.cache_dir / "metadata.json"
        self._load_metadata()
    
    def _load_metadata(self):
        """Carrega metadados do cache"""
        try:
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r') as f:
                    self.metadata = json.load(f)
            else:
                self.metadata = {
                    'last_update': None,
                    'cache_hits': 0,
                    'cache_misses': 0,
                    'cache_size': 0
                }
        except Exception as e:
            logger.error(f"Erro ao carregar metadados: {e}")
            self.metadata = {
                'last_update': None,
                'cache_hits': 0,
                'cache_misses': 0,
                'cache_size': 0
            }
    
    def _save_metadata(self):
        """Salva metadados do cache"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar metadados: {e}")
    
    def _generate_cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Gera chave única para cache"""
        key_data = f"{prefix}:{str(args)}:{str(kwargs)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, prefix: str, *args, **kwargs) -> Optional[Any]:
        """Obtém item do cache"""
        cache_key = self._generate_cache_key(prefix, *args, **kwargs)
        
        # Verificar cache em memória primeiro
        if cache_key in self.memory_cache:
            item = self.memory_cache[cache_key]
            if datetime.now() < item['expires_at']:
                self.metadata['cache_hits'] += 1
                logger.debug(f"Cache hit: {prefix}")
                return item['data']
            else:
                # Item expirado, remover
                del self.memory_cache[cache_key]
        
        # Verificar cache em disco
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    item = pickle.load(f)
                
                if datetime.now() < item['expires_at']:
                    # Carregar para memória
                    self.memory_cache[cache_key] = item
                    self.metadata['cache_hits'] += 1
                    self._save_metadata()
                    logger.debug(f"Cache hit (disco): {prefix}")
                    return item['data']
                else:
                    # Item expirado, remover
                    cache_file.unlink(missing_ok=True)
            except Exception as e:
                logger.error(f"Erro ao ler cache do disco: {e}")
                cache_file.unlink(missing_ok=True)
        
        self.metadata['cache_misses'] += 1
        self._save_metadata()
        logger.debug(f"Cache miss: {prefix}")
        return None
    
    def set(self, prefix: str, data: Any, ttl: Optional[int] = None, *args, **kwargs):
        """Armazena item no cache"""
        cache_key = self._generate_cache_key(prefix, *args, **kwargs)
        
        if ttl is None:
            ttl = self.cache_ttl
        
        cache_item = {
            'data': data,
            'created_at': datetime.now(),
            'expires_at': datetime.now() + timedelta(seconds=ttl),
            'prefix': prefix,
            'key': cache_key
        }
        
        # Armazenar em memória
        self.memory_cache[cache_key] = cache_item
        
        # Armazenar em disco (assíncrono)
        try:
            cache_file = self.cache_dir / f"{cache_key}.pkl"
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_item, f)
            
            # Atualizar tamanho do cache
            self.metadata['cache_size'] = sum(
                f.stat().st_size for f in self.cache_dir.glob("*.pkl")
            )
            self._save_metadata()
            
            logger.debug(f"Cache set: {prefix} (TTL: {ttl}s)")
        except Exception as e:
            logger.error(f"Erro ao salvar cache no disco: {e}")
    
    def clear(self, prefix: Optional[str] = None):
        """Limpa cache"""
        if prefix is None:
            # Limpar tudo
            self.memory_cache.clear()
            
            # Limpar arquivos de cache
            for cache_file in self.cache_dir.glob("*.pkl"):
                try:
                    cache_file.unlink()
                except:
                    pass
            
            logger.info("Cache limpo completamente")
        else:
            # Limpar apenas itens com prefixo específico
            keys_to_remove = [
                key for key, item in self.memory_cache.items()
                if item['prefix'] == prefix
            ]
            
            for key in keys_to_remove:
                del self.memory_cache[key]
                
                # Remover do disco
                cache_file = self.cache_dir / f"{key}.pkl"
                cache_file.unlink(missing_ok=True)
            
            logger.info(f"Cache limpo para prefixo: {prefix}")
        
        # Resetar metadados
        self.metadata['cache_size'] = 0
        self._save_metadata()
    
    def should_refresh(self) -> bool:
        """Verifica se os dados devem ser atualizados"""
        if 'last_update' not in self.metadata or not self.metadata['last_update']:
            return True
        
        try:
            last_update = datetime.fromisoformat(self.metadata['last_update'])
            refresh_interval = timedelta(days=settings.ANP_UPDATE_INTERVAL_DAYS)
            
            should_refresh = datetime.now() - last_update > refresh_interval
            
            if should_refresh:
                logger.info(
                    f"Dados expirados. Última atualização: {last_update}. "
                    f"Intervalo de atualização: {settings.ANP_UPDATE_INTERVAL_DAYS} dias"
                )
            
            return should_refresh
            
        except Exception as e:
            logger.error(f"Erro ao verificar necessidade de atualização: {e}")
            return True
    
    def update_timestamp(self):
        """Atualiza timestamp da última atualização"""
        self.metadata['last_update'] = datetime.now().isoformat()
        self._save_metadata()
        logger.info(f"Timestamp atualizado: {self.metadata['last_update']}")
    
    def get_timestamp(self) -> Optional[datetime]:
        """Obtém timestamp da última atualização"""
        if 'last_update' in self.metadata and self.metadata['last_update']:
            try:
                return datetime.fromisoformat(self.metadata['last_update'])
            except:
                return None
        return None
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas do cache"""
        cache_files = list(self.cache_dir.glob("*.pkl"))
        
        return {
            'memory_cache_size': len(self.memory_cache),
            'disk_cache_files': len(cache_files),
            'disk_cache_size_bytes': sum(f.stat().st_size for f in cache_files),
            'cache_hits': self.metadata.get('cache_hits', 0),
            'cache_misses': self.metadata.get('cache_misses', 0),
            'hit_ratio': (
                self.metadata['cache_hits'] / 
                (self.metadata['cache_hits'] + self.metadata['cache_misses'])
                if (self.metadata['cache_hits'] + self.metadata['cache_misses']) > 0
                else 0
            ),
            'last_update': self.metadata.get('last_update'),
            'should_refresh': self.should_refresh()
        }

# Instância global do cache
cache = CacheManager()
