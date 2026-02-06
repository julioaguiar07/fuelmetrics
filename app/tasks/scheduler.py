from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
import logging
from app.services.anp_downloader import ANPDownloader
from app.services.cache_manager import cache
from app.config import settings

logger = logging.getLogger(__name__)

# Agendador global
scheduler = BackgroundScheduler()
scheduler_initialized = False

def update_anp_data():
    """Tarefa para atualizar dados da ANP"""
    logger.info("Iniciando atualização programada dos dados da ANP...")
    
    try:
        downloader = ANPDownloader()
        
        # Forçar download
        filepath = downloader.download_file(force=True)
        
        if filepath:
            # Limpar cache para forçar recálculo
            cache.clear()
            cache.update_timestamp()
            
            logger.info(f"Dados da ANP atualizados com sucesso: {filepath}")
            
            # Log de estatísticas
            try:
                import pandas as pd
                df = pd.read_excel(filepath, nrows=5)
                logger.info(f"Arquivo válido com {len(df.columns)} colunas")
            except Exception as e:
                logger.warning(f"Erro ao validar arquivo após atualização: {e}")
        else:
            logger.error("Falha ao baixar dados da ANP")
            
    except Exception as e:
        logger.error(f"Erro na atualização programada: {e}")
        # Não propagar exceção para não interromper o agendador

def cleanup_old_data():
    """Limpeza de dados antigos"""
    logger.info("Iniciando limpeza de dados antigos...")
    
    try:
        import os
        from pathlib import Path
        from datetime import datetime
        
        data_dir = Path("data")
        if data_dir.exists():
            for file in data_dir.glob("*.xlsx"):
                file_age = datetime.now() - datetime.fromtimestamp(file.stat().st_mtime)
                if file_age.days > 30:  # Manter apenas 30 dias de dados
                    logger.info(f"Removendo arquivo antigo: {file.name}")
                    file.unlink()
        
        # Limpar cache antigo
        cache_files = list(Path("cache").glob("*.pkl"))
        for cache_file in cache_files:
            file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if file_age.days > 7:  # Manter cache por 7 dias
                logger.info(f"Removendo cache antigo: {cache_file.name}")
                cache_file.unlink()
        
        logger.info("Limpeza concluída")
        
    except Exception as e:
        logger.error(f"Erro na limpeza de dados: {e}")

def health_check():
    """Verificação de saúde do sistema"""
    logger.info("Executando verificação de saúde...")
    
    try:
        # Verificar espaço em disco
        import shutil
        total, used, free = shutil.disk_usage("/")
        
        disk_info = {
            'total_gb': total / (1024**3),
            'used_gb': used / (1024**3),
            'free_gb': free / (1024**3),
            'free_percent': (free / total) * 100
        }
        
        logger.info(
            f"Espaço em disco: {disk_info['free_gb']:.1f}GB livres "
            f"({disk_info['free_percent']:.1f}%)"
        )
        
        # Verificar memória (Linux)
        try:
            import psutil
            memory = psutil.virtual_memory()
            logger.info(
                f"Memória: {memory.percent}% usada, "
                f"{memory.available / (1024**3):.1f}GB disponível"
            )
        except:
            pass
        
        # Verificar cache
        cache_stats = cache.get_stats()
        logger.info(
            f"Cache: {cache_stats['hit_ratio']:.1%} hit ratio, "
            f"{cache_stats['memory_cache_size']} itens em memória"
        )
        
    except Exception as e:
        logger.error(f"Erro na verificação de saúde: {e}")

def start_scheduler():
    """Inicia o agendador de tarefas"""
    global scheduler_initialized
    
    if scheduler_initialized:
        logger.warning("Agendador já está inicializado")
        return
    
    try:
        logger.info("Inicializando agendador de tarefas...")
        
        # Atualização semanal dos dados (segunda-feira às 03:00)
        scheduler.add_job(
            update_anp_data,
            trigger=CronTrigger(day_of_week='mon', hour=3, minute=0),
            id='update_anp_data',
            name='Atualização semanal dos dados da ANP',
            replace_existing=True
        )
        
        # Limpeza diária (02:00)
        scheduler.add_job(
            cleanup_old_data,
            trigger=CronTrigger(hour=2, minute=0),
            id='cleanup_old_data',
            name='Limpeza de dados antigos',
            replace_existing=True
        )
        
        # Health check a cada 6 horas
        scheduler.add_job(
            health_check,
            trigger=CronTrigger(hour='*/6'),
            id='health_check',
            name='Verificação de saúde do sistema',
            replace_existing=True
        )
        
        # Iniciar agendador
        scheduler.start()
        scheduler_initialized = True
        
        logger.info("Agendador inicializado com sucesso")
        
        # Executar tarefas imediatamente na inicialização (apenas desenvolvimento)
        if settings.ENVIRONMENT == 'development':
            logger.info("Executando tarefas iniciais...")
            update_anp_data()
        
        # Log de jobs agendados
        jobs = scheduler.get_jobs()
        logger.info(f"{len(jobs)} tarefas agendadas:")
        for job in jobs:
            logger.info(f"  - {job.name} (próxima execução: {job.next_run_time})")
        
    except Exception as e:
        logger.error(f"Erro ao inicializar agendador: {e}")
        raise

def shutdown_scheduler():
    """Desliga o agendador de tarefas"""
    global scheduler_initialized
    
    if scheduler_initialized:
        try:
            logger.info("Desligando agendador...")
            scheduler.shutdown(wait=False)
            scheduler_initialized = False
            logger.info("Agendador desligado")
        except Exception as e:
            logger.error(f"Erro ao desligar agendador: {e}")

def get_scheduler_status() -> dict:
    """Retorna status do agendador"""
    if not scheduler_initialized:
        return {'status': 'not_initialized'}
    
    try:
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        
        return {
            'status': 'running',
            'job_count': len(jobs),
            'jobs': jobs,
            'initialized': scheduler_initialized
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }

# Função para teste manual
if __name__ == "__main__":
    import sys
    from app.utils.logger import setup_logging
    
    setup_logging()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'update':
            update_anp_data()
        elif sys.argv[1] == 'cleanup':
            cleanup_old_data()
        elif sys.argv[1] == 'health':
            health_check()
        else:
            print("Comandos disponíveis: update, cleanup, health")
    else:
        print("Especifica um comando: update, cleanup, health")
