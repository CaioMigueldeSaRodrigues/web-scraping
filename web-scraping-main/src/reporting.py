from src.logger_config import logger
import pandas as pd
from typing import Optional

def generate_reports() -> None:
    """
    Gera relatórios em Excel e HTML a partir dos dados de benchmarking.
    """
    try:
        logger.info("Gerando relatórios de benchmarking...")
        # Exemplo: carregar pares, gerar Excel/HTML
        # ...
        logger.info("Relatórios gerados.")
    except Exception as e:
        logger.error(f"Erro ao gerar relatórios: {e}")
        raise 