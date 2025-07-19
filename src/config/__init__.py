"""
Configurações do projeto de Web Scraping - Bemol vs Magalu

Este módulo contém todas as configurações centralizadas do projeto,
incluindo thresholds, parâmetros de email e configurações gerais.
"""

from .config import (
    SIMILARIDADE_THRESHOLD,
    PRECO_THRESHOLD,
    EMAIL_CONFIG,
    LOGGING_CONFIG
)

__all__ = [
    "SIMILARIDADE_THRESHOLD",
    "PRECO_THRESHOLD", 
    "EMAIL_CONFIG",
    "LOGGING_CONFIG"
] 