import logging
import sys
from typing import Optional


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Configura e retorna um logger padronizado para o projeto.
    
    Args:
        name: Nome do logger (geralmente __name__)
        level: Nível de log (opcional)
        
    Returns:
        logging.Logger: Logger configurado
    """
    logger = logging.getLogger(name)
    
    # Evita adicionar handlers duplicados
    if logger.handlers:
        return logger
    
    # Define nível padrão se não especificado
    if level is None:
        level = logging.INFO
    
    logger.setLevel(level)
    
    # Cria formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para arquivo (opcional - para Databricks)
    try:
        file_handler = logging.FileHandler('/tmp/web_scraping.log')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        # Se não conseguir criar arquivo, continua apenas com console
        pass
    
    return logger


def configure_root_logger(level: int = logging.INFO) -> None:
    """
    Configura o logger root para o projeto.
    
    Args:
        level: Nível de log
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def set_log_level(level: str) -> None:
    """
    Define o nível de log global.
    
    Args:
        level: Nível de log ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    """
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    if level.upper() in level_map:
        logging.getLogger().setLevel(level_map[level.upper()])
        for handler in logging.getLogger().handlers:
            handler.setLevel(level_map[level.upper()])
    else:
        raise ValueError(f"Nível de log inválido: {level}. Use: DEBUG, INFO, WARNING, ERROR, CRITICAL") 