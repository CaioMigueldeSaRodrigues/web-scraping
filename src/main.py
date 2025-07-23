import sys
import os
import logging
from pyspark.sql import SparkSession
from src.config import SILVER_TABLE_NAME
from src.modules.scraper_logic import scrape_magazine_luiza
from src.scraping import load_scraped_data, load_databricks_table
from src.data_processing import generate_embeddings, find_similar_products, format_report_for_business
from src.reporting import generate_business_report_excel, generate_analytical_report_excel, generate_html_report, send_email_report

def run_pipeline():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- INICIANDO PIPELINE DE ANÁLISE DE CONCORRÊNCIA V2.4 ---")
    spark = SparkSession.builder.appName("AnaliseConcorrenciaPipeline").enableHiveSupport().getOrCreate()

    # 1. Extração de Dados
    scrape_magazine_luiza(spark)
    df_site_spark = load_scraped_data(spark)
    df_tabela_spark = load_databricks_table(spark)
    df_site_pandas = df_site_spark.toPandras()
    df_tabela_pandas = df_tabela_spark.toPandas()

    # 2. Geração de Embeddings
    df_site_embedded = generate_embeddings(df_site_pandas, 'titulo_site')
    df_tabela_embedded = generate_embeddings(df_tabela_pandas, 'titulo_tabela')

    # 3. Geração do Relatório Analítico (Pareamento)
    analytical_report_df = find_similar_products(df_site_embedded, df_tabela_embedded)
    if analytical_report_df.empty:
        logging.warning("Nenhum resultado gerado na análise. Encerrando pipeline.")
        return

    # --- AÇÃO: SALVAR O RESULTADO COMO TABELA DELTA ---
    logging.info(f"Salvando resultado analítico na tabela Delta: {SILVER_TABLE_NAME}")
    spark_analytical_df = spark.createDataFrame(analytical_report_df)
    spark_analytical_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE_NAME)
    logging.info("Tabela Delta salva com sucesso.")
    # ---------------------------------------------------

    # 4. Formatação do Relatório de Negócios
    business_report_df = format_report_for_business(analytical_report_df)
    
    # 5. Geração de Outputs para as equipes
    business_excel = generate_business_report_excel(business_report_df)
    analytical_excel = generate_analytical_report_excel(analytical_report_df)
    html_content = generate_html_report(business_report_df)

    # 6. Envio de Email com ambos os anexos
    send_email_report(html_content, business_excel, analytical_excel)

    logging.info("--- PIPELINE CONCLUÍDO ---")
    spark.stop() 