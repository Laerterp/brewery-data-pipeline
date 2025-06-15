import logging
import sys
from typing import Optional

def get_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Configura e retorna um logger com o nome especificado
    
    Args:
        name: Nome do logger (geralmente __name__)
        log_level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Objeto logger configurado
    """
    logger = logging.getLogger(name)
    
    # Evita adicionar handlers múltiplos
    if not logger.handlers:
        logger.setLevel(log_level)
        
        # Formato do log
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para stdout
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger