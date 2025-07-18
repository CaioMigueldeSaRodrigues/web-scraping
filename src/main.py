# src/main.py
# Este é o script principal que será executado pelo Job no Databricks.

from pyspark.sql import SparkSession
from src.embeddings import generate_embeddings # Importa a função de embeddings
# from src.config import get_secret, SECRET_SCOPE, SENDGRID_API_KEY_NAME # Se precisar de config aqui

def run_pipeline():
    spark = SparkSession.builder \
        .appName("Benchmarking Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.deltaCatalog") \
        .getOrCreate()

    # Exemplo de chamada da função de embeddings
    # Adapte os nomes das tabelas e o batch_size conforme necessário
    generate_embeddings(
        spark=spark,
        source_table="bronze.magalu_completo",
        target_table="silver.embeddings_magalu_completo",
        batch_size=250 # Seu tamanho de batch desejado
    )

    # Adicione aqui as chamadas para as outras etapas do seu pipeline:
    # - Processamento de dados da Bemol
    # - Cálculo de similaridade
    # - Geração de relatórios
    # - Envio de e-mail

    spark.stop() # Parar a sessão Spark ao final

if __name__ == "__main__":
    run_pipeline() 