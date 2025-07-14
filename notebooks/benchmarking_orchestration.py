# Databricks notebook source
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

from src.scraping import run_scraping_pipeline
from src.embeddings import generate_embeddings
from src.matching import match_products
from src.reporting import generate_reports
from src.emailer import send_benchmarking_email
from src.logger_config import logger

# Orquestração do pipeline completo
try:
    logger.info("Iniciando pipeline de benchmarking...")
    run_scraping_pipeline()
    generate_embeddings()
    match_products()
    generate_reports()
    send_benchmarking_email()
    logger.info("Pipeline concluído com sucesso!")
except Exception as e:
    logger.error(f"Erro na orquestração do pipeline: {e}")
    raise 