import sys
import os
import logging
from pyspark.sql import SparkSession
from src.scraping import scrape_and_save_all_categories, load_scraped_data, load_databricks_table
from src.data_processing import generate_embeddings, find_similar_products, format_report_for_business
from src.reporting import generate_business_report_excel, generate_analytical_report_excel, generate_html_report, send_email_report

def run_pipeline():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- INICIANDO PIPELINE DE ANÁLISE DE CONCORRÊNCIA V2.2 ---")
    spark = SparkSession.builder.appName("AnaliseConcorrenciaPipeline").enableHiveSupport().getOrCreate()

    # 1. Extração de Dados
    scrape_and_save_all_categories(spark)
    df_site_spark = load_scraped_data(spark)
    df_tabela_spark = load_databricks_table(spark)
    df_site_pandas = df_site_spark.toPandas()
    df_tabela_pandas = df_tabela_spark.toPandas()

    # 2. Geração de Embeddings
    df_site_embedded = generate_embeddings(df_site_pandas, 'titulo_site')
    df_tabela_embedded = generate_embeddings(df_tabela_pandas, 'titulo_tabela')

    # 3. Geração do Relatório Analítico (Produto 1: Dados para BI e Depuração)
    analytical_report_df = find_similar_products(df_site_embedded, df_tabela_embedded)
    if analytical_report_df.empty:
        logging.warning("Nenhum resultado gerado na análise. Encerrando pipeline.")
        return

    # 4. Formatação do Relatório de Negócios (Produto 2: Planilha para equipes)
    business_report_df = format_report_for_business(analytical_report_df)
    
    # 5. Geração de Outputs para as equipes
    business_excel = generate_business_report_excel(business_report_df)
    analytical_excel = generate_analytical_report_excel(analytical_report_df)
    html_content = generate_html_report(business_report_df)

    # 6. Envio de Email com ambos os anexos
    send_email_report(html_content, business_excel, analytical_excel)

    logging.info("--- PIPELINE CONCLUÍDO ---")
    spark.stop() 