# Databricks notebook source

# COMMAND ----------

# Pipeline direto para Databricks - sem dependências de importação
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType

# Forçar CPU para evitar erros de CUDA
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
os.environ['TRANSFORMERS_NO_ADAM'] = '1'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

print("🚀 Pipeline iniciado no Databricks")

# COMMAND ----------

# Configurar SparkSession
spark = SparkSession.builder \
    .appName("Benchmarking Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.deltaCatalog") \
    .getOrCreate()

print("✅ SparkSession configurado")

# COMMAND ----------

try:
    # Verificar se a tabela existe
    df = spark.sql("SELECT COUNT(*) as total FROM bronze.magalu_completo")
    count = df.collect()[0]['total']
    print(f"✅ Tabela bronze.magalu_completo encontrada com {count} registros")
    
    # Processamento de embeddings
    print("📊 Processamento de embeddings iniciado...")
    
    # Carregar dados
    df_data = spark.sql("SELECT title, price, url, categoria FROM bronze.magalu_completo").toPandas()
    print(f"📊 Carregados {len(df_data)} registros para processamento")
    
    # Gerar embeddings
    from sentence_transformers import SentenceTransformer
    modelo = SentenceTransformer("all-MiniLM-L6-v2", device='cpu')
    print("✅ Modelo SentenceTransformer carregado com sucesso")
    
    embeddings = []
    titles = df_data['title'].tolist()
    batch_size = 250
    
    print(f"🔄 Processando {len(titles)} títulos em batches de {batch_size}...")
    
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i+batch_size]
        vectors = modelo.encode(batch, show_progress_bar=True, convert_to_tensor=False, device='cpu')
        embeddings.extend(vectors)
        print(f"✅ Batch {i//batch_size + 1}/{(len(titles) + batch_size - 1)//batch_size} processado")
    
    # Adicionar embeddings
    df_data["embedding"] = embeddings
    print("✅ Embeddings adicionados ao DataFrame")
    
    # Schema
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("url", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("embedding", ArrayType(FloatType()), True)
    ])
    
    # Criar DataFrame Spark
    spark_df = spark.createDataFrame(df_data, schema=schema)
    print("✅ DataFrame Spark criado")
    
    # Salvar tabela Delta
    spark_df.write.format("delta").mode("overwrite").saveAsTable("silver.embeddings_magalu_completo")
    
    print("✅ silver.embeddings_magalu_completo criada com sucesso!")
    print("✅ Pipeline executado com sucesso!")
    
except Exception as e:
    print(f"❌ Erro durante execução: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# Parar a sessão Spark
spark.stop()
print("✅ SparkSession finalizada") 