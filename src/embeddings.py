import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from sentence_transformers import SentenceTransformer
import torch # Importa torch para gerenciar dispositivo

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Forçar o uso de CPU e desabilitar CUDA antes de qualquer importação de torch ou sentence_transformers
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
os.environ['TRANSFORMERS_NO_ADAM'] = '1'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

def generate_embeddings(spark, source_table, target_table, batch_size=250):
    """
    Função genérica para gerar embeddings de qualquer tabela.
    
    Args:
        spark: SparkSession
        source_table: Nome da tabela fonte (ex: "bronze.magalu_completo")
        target_table: Nome da tabela destino (ex: "silver.embeddings_magalu_completo")
        batch_size: Tamanho do batch para processamento (padrão: 250)
    """
    # Carregue os dados
    df = spark.sql(f"SELECT title, price, url, categoria FROM {source_table}").toPandas()

    # Inicialize o modelo com CPU
    modelo = SentenceTransformer("all-MiniLM-L6-v2", device='cpu')

    # Realize o embedding em batch
    embeddings = []
    titles = df['title'].tolist()
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i+batch_size]
        vectors = modelo.encode(batch, show_progress_bar=True, convert_to_tensor=False, device='cpu')
        embeddings.extend(vectors)

    # Adicione os embeddings ao DataFrame
    df["embedding"] = embeddings

    # Defina o schema
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("url", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("embedding", ArrayType(FloatType()), True)
    ])

    # Crie o DataFrame Spark
    spark_df = spark.createDataFrame(df, schema=schema)

    # Salve o DataFrame como tabela Delta
    spark_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

    print(f"✅ {target_table} criada com sucesso.")

def generate_magalu_embeddings(spark):
    """Função específica para embeddings do Magalu"""
    generate_embeddings(
        spark=spark,
        source_table="bronze.magalu_completo",
        target_table="silver.embeddings_magalu_completo"
    )

def generate_tabela_embeddings(spark):
    """Função específica para embeddings da Tabela"""
    generate_embeddings(
        spark=spark,
        source_table="bronze.tabela_completo",
        target_table="silver.embeddings_tabela_completo"
    ) 