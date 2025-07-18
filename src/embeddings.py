import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from sentence_transformers import SentenceTransformer
import torch # Importa torch para gerenciar dispositivo

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_magalu_embeddings(spark):
    # Carregue os dados
    df = spark.sql("SELECT title, price, url, categoria FROM bronze.magalu_completo").toPandas()

    # Inicialize o modelo com CPU
    modelo = SentenceTransformer("all-MiniLM-L6-v2", device='cpu')

    # Defina o batch size
    batch_size = 250

    # Realize o embedding em batch
    embeddings = []
    for i in range(0, len(df), batch_size):
        batch = df['title'].iloc[i:i+batch_size]
        embeddings.extend(modelo.encode(batch, show_progress_bar=False, convert_to_tensor=False))

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
    spark_df.write.format("delta").mode("overwrite").saveAsTable("silver.embeddings_magalu_completo")

    print("✅ silver.embeddings_magalu_completo criada com sucesso.")

def generate_tabela_embeddings(spark):
    # Carregue os dados
    df = spark.sql("SELECT title, price, url, categoria FROM bronze.tabela_completo").toPandas()

    # Inicialize o modelo com CPU
    modelo = SentenceTransformer("all-MiniLM-L6-v2", device='cpu')

    # Defina o batch size
    batch_size = 250

    # Realize o embedding em batch
    embeddings = []
    for i in range(0, len(df), batch_size):
        batch = df['title'].iloc[i:i+batch_size]
        embeddings.extend(modelo.encode(batch, show_progress_bar=False, convert_to_tensor=False))

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
    spark_df.write.format("delta").mode("overwrite").saveAsTable("silver.embeddings_tabela_completo")

    print("✅ silver.embeddings_tabela_completo criada com sucesso.") 