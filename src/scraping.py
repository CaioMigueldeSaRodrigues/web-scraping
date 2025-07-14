from src.logger_config import logger
import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict

def run_scraping_pipeline() -> None:
    """
    Executa o scraping dos marketplaces definidos e salva os dados brutos.
    """
    try:
        logger.info("Iniciando scraping dos marketplaces...")
        # Exemplo: scraping Magalu (adapte para múltiplos marketplaces)
        # TODO: Modularizar scraping de cada marketplace
        # ...
        logger.info("Scraping finalizado.")
    except Exception as e:
        logger.error(f"Erro no scraping: {e}")
        raise 