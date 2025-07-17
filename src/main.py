from pyspark.sql import SparkSession
import logging
from src.scraping import scrape_and_save_all_categories, load_scraped_data, load_databricks_table
from src.data_processing import generate_embeddings, find_similar_products
from src.reporting import generate_excel_report, generate_html_report, send_email_report

def run_pipeline():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- INICIANDO PIPELINE DE ANÁLISE DE CONCORRÊNCIA V2.0 ---")

    spark = SparkSession.builder.appName("AnaliseConcorrenciaPipeline").enableHiveSupport().getOrCreate()

    # 1. Extração de Dados
    scrape_and_save_all_categories(spark)
    df_site_spark = load_scraped_data(spark)
    df_tabela_spark = load_databricks_table(spark)
    
    # Converão para Pandas para processamento de NLP
    df_site_pandas = df_site_spark.toPandas()
    df_tabela_pandas = df_tabela_spark.toPandas()

    # 2. Pré-processamento e Embeddings
    df_site_embedded = generate_embeddings(df_site_pandas, 'titulo_site')
    df_tabela_embedded = generate_embeddings(df_tabela_pandas, 'titulo_tabela')

    # 3. Comparação e Geração do Relatório Final
    final_report_df = find_similar_products(df_site_embedded, df_tabela_embedded)
    if final_report_df.empty:
        logging.warning("Nenhum resultado gerado. Encerrando pipeline.")
        return

    # 4. Geração de Outputs
    excel_file = generate_excel_report(final_report_df)
    html_content = generate_html_report(final_report_df)

    # 5. Envio de Email
    send_email_report(html_content, excel_file)

    logging.info("--- PIPELINE CONCLUÍDO ---")
    spark.stop()

if __name__ == "__main__":
    run_pipeline() 