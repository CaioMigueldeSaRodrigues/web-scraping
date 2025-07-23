# Databricks notebook source
# MAGIC %run ./src/config.py

# COMMAND ----------

# MAGIC %run ./src/scraping.py

# COMMAND ----------

# MAGIC %run ./src/data_processing.py

# COMMAND ----------

# MAGIC %run ./src/reporting.py

# COMMAND ----------

import logging
from pyspark.sql import SparkSession

def main():
    """Função principal que orquestra o pipeline."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- INICIANDO PIPELINE DE BENCHMARKING (MODELO %run) ---")
    
    spark = SparkSession.builder.appName("BenchmarkingPipeline").enableHiveSupport().getOrCreate()
    
    # Define o dicionário de configuração a ser passado para as funções
    config = {
        "MAGALU_CATEGORIES": MAGALU_CATEGORIES,
        "MAX_PAGES_PER_CATEGORY": MAX_PAGES_PER_CATEGORY,
        "BRONZE_LAYER_PATH": BRONZE_LAYER_PATH,
        "DATABRICKS_TABLE": DATABRICKS_TABLE,
        "EMBEDDING_MODEL": EMBEDDING_MODEL,
        "SIMILARITY_THRESHOLD": SIMILARITY_THRESHOLD,
        "SENDGRID_API_KEY": SENDGRID_API_KEY,
        "FROM_EMAIL": FROM_EMAIL,
        "TO_EMAILS": TO_EMAILS,
        "EMAIL_SUBJECT": EMAIL_SUBJECT
    }
    
    # 1. Extração
    df_site_spark, df_tabela_spark = execute_scraping_and_load(spark, config)
    
    # 2. Processamento e Pareamento
    analytical_report_df, business_report_df = execute_processing(df_site_spark, df_tabela_spark, config)
    
    # 3. Geração de Relatórios e Envio
    if not business_report_df.empty:
        execute_reporting(analytical_report_df, business_report_df, config)
    else:
        logging.warning("Nenhum dado processado para gerar relatórios.")
        
    logging.info("--- PIPELINE CONCLUÍDO ---")

# COMMAND ----------

if __name__ == "__main__":
    main()
