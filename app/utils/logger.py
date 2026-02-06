import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from datetime import datetime
from app.config import settings

def setup_logging():
    """Configura o sistema de logging da aplicação"""
    
    # Criar diretório de logs se não existir
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Nome do arquivo de log com timestamp
    log_filename = log_dir / f"fuelmetrics_{datetime.now().strftime('%Y%m')}.log"
    
    # Configurar formato do log
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - '
        '[%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configurar nível de log
    log_level = getattr(logging, settings.LOG_LEVEL.upper())
    
    # Configurar handler para arquivo
    file_handler = TimedRotatingFileHandler(
        filename=log_filename,
        when='D',  # Rotação diária
        interval=1,
        backupCount=30,  # Manter 30 dias de logs
        encoding='utf-8'
    )
    file_handler.setFormatter(log_format)
    file_handler.setLevel(log_level)
    
    # Configurar handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    console_handler.setLevel(log_level)
    
    # Configurar logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remover handlers existentes
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Adicionar novos handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Configurar loggers de terceiros
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)
    
    # Log inicial
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("Sistema de logging configurado")
    logger.info(f"Level: {settings.LOG_LEVEL}")
    logger.info(f"Arquivo: {log_filename}")
    logger.info("=" * 60)

def get_logger(name: str) -> logging.Logger:
    """Retorna um logger configurado"""
    return logging.getLogger(name)

class RequestLogger:
    """Middleware de logging para requisições"""
    
    @staticmethod
    def log_request(request, response=None, exception=None):
        """Log detalhado de requisições"""
        logger = logging.getLogger('request')
        
        log_data = {
            'method': request.method,
            'path': request.url.path,
            'client_ip': request.client.host if request.client else 'unknown',
            'user_agent': request.headers.get('user-agent', 'unknown')
        }
        
        if response:
            log_data['status_code'] = response.status_code
            log_data['response_time'] = getattr(request.state, 'response_time', 0)
        
        if exception:
            log_data['exception'] = str(exception)
        
        if response and response.status_code >= 500:
            logger.error(log_data)
        elif response and response.status_code >= 400:
            logger.warning(log_data)
        else:
            logger.info(log_data)

class PerformanceLogger:
    """Logger para métricas de performance"""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = datetime.now()
        self.logger = logging.getLogger('performance')
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds() * 1000
        
        log_data = {
            'operation': self.operation_name,
            'duration_ms': duration,
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'success': exc_type is None
        }
        
        if exc_type is not None:
            log_data['exception'] = str(exc_val)
            self.logger.error(log_data)
        elif duration > 1000:  # Mais de 1 segundo
            self.logger.warning(log_data)
        else:
            self.logger.info(log_data)
