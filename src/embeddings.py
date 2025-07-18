import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from sentence_transformers import SentenceTransformer
import torch # Importa torch para gerenciar dispositivo

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_embeddings(spark: SparkSession, source_table: str, target_table: str, batch_size: int = 250):
    """
    Gera embeddings para os títulos dos produtos de uma tabela Delta e salva em uma nova tabela.
    Força o uso da CPU para o modelo SentenceTransformer.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        source_table (str): Nome da tabela Delta de origem (ex: "bronze.magalu_completo").
        target_table (str): Nome da tabela Delta de destino para os embeddings (ex: "silver.embeddings_magalu_completo").
        batch_size (int): Tamanho do batch para processamento dos embeddings.
    """
    logging.info(f"Iniciando a geração de embeddings para a tabela '{source_table}'.")

    # --- FORÇA O USO DA CPU ---
    # Garante que o PyTorch (usado por SentenceTransformer) não tente usar GPU
    os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
    device = torch.device("cpu")
    logging.info(f"Dispositivo de processamento forçado para: {device}")

    # Carrega os dados da tabela de origem
    try:
        df_spark = spark.sql(f"SELECT title, price, url, categoria FROM {source_table}")
        df_pandas = df_spark.toPandas()
        logging.info(f"Dados da tabela '{source_table}' carregados. Total de registros: {len(df_pandas)}")
    except Exception as e:
        logging.error(f"Erro ao carregar dados da tabela '{source_table}': {e}")
        raise

    # Remove nulos e strings vazias do título para evitar erros no embedding
    df_pandas = df_pandas[df_pandas['title'].notnull() & (df_pandas['title'].str.strip() != "")].reset_index(drop=True)
    logging.info(f"Registros válidos após limpeza de títulos: {len(df_pandas)}")

    if df_pandas.empty:
        logging.warning("DataFrame vazio após limpeza de títulos. Nenhuma embedding será gerada.")
        # Cria um DataFrame Spark vazio com o schema correto para a tabela de destino
        empty_schema = StructType([
            StructField("title", StringType(), True),
            StructField("price", StringType(), True),
            StructField("url", StringType(), True),
            StructField("categoria", StringType(), True),
            StructField("embedding", ArrayType(FloatType()), True)
        ])
        spark.createDataFrame(pd.DataFrame(columns=empty_schema.fieldNames()), schema=empty_schema).write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)
        logging.info(f"Tabela vazia '{target_table}' criada com sucesso.")
        return

    # Carrega o modelo de embedding, garantindo o uso da CPU
    try:
        modelo = SentenceTransformer("all-MiniLM-L6-v2", device=device)
        logging.info("Modelo SentenceTransformer 'all-MiniLM-L6-v2' carregado com sucesso na CPU.")
    except Exception as e:
        logging.error(f"Erro ao carregar o modelo SentenceTransformer: {e}")
        raise

    # Geração de embeddings em batches
    embeddings = []
    titles_list = df_pandas["title"].tolist()
    
    for i in range(0, len(titles_list), batch_size):
        batch = titles_list[i:i + batch_size]
        try:
            vectors = modelo.encode(batch, show_progress_bar=False).tolist() # show_progress_bar para evitar logs excessivos
            embeddings.extend(vectors)
            logging.info(f"Processado batch {i // batch_size + 1}/{(len(titles_list) + batch_size - 1) // batch_size}. Total embeddings: {len(embeddings)}")
        except Exception as e:
            logging.error(f"Erro ao processar batch {i // batch_size + 1}: {e}")
            # Decide como lidar com erros de batch: pular, preencher com None, etc.
            # Por simplicidade, vamos estender com None para o tamanho do batch em caso de erro
            embeddings.extend([None] * len(batch)) # Ou uma estratégia mais robusta

    df_pandas["embedding"] = embeddings
    logging.info(f"Geração de embeddings concluída. Total de embeddings gerados: {df_pandas['embedding'].notnull().sum()}")

    # Define o schema para o DataFrame Spark
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("url", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("embedding", ArrayType(FloatType()), True)
    ])

    # Converte para Spark DataFrame e salva como tabela Delta
    try:
        spark_df = spark.createDataFrame(df_pandas, schema=schema)
        spark_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        logging.info(f"✅ Tabela '{target_table}' criada com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao salvar a tabela '{target_table}': {e}")
        raise 