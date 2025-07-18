import os
import logging
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Dict, Any, Tuple
from .logger_config import get_logger
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

logger = get_logger(__name__)


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


def calcular_similaridade_embeddings(
    df_magalu: pd.DataFrame, 
    df_bemol: pd.DataFrame
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Calcula matriz de similaridade entre embeddings do Magalu e Bemol.
    
    Args:
        df_magalu: DataFrame do Magalu com embeddings
        df_bemol: DataFrame da Bemol com embeddings
        
    Returns:
        Tuple: (matriz_similaridade, indices_melhor_match, scores_melhor_match)
    """
    try:
        # Converte embeddings para lista de arrays
        embeddings_magalu = df_magalu["embedding"].tolist()
        embeddings_bemol = df_bemol["embedding"].tolist()
        
        # Calcula matriz de similaridade
        sim_matrix = cosine_similarity(embeddings_magalu, embeddings_bemol)
        
        # Encontra melhor match para cada produto do Magalu
        matched_indices = sim_matrix.argmax(axis=1)
        matched_scores = sim_matrix.max(axis=1)
        
        logger.info(f"Similaridade calculada: {len(df_magalu)} produtos Magalu vs {len(df_bemol)} produtos Bemol")
        
        return sim_matrix, matched_indices, matched_scores
        
    except Exception as e:
        logger.error(f"Erro ao calcular similaridade: {e}")
        raise


def criar_pares_produtos(
    df_magalu: pd.DataFrame,
    df_bemol: pd.DataFrame,
    matched_indices: np.ndarray,
    matched_scores: np.ndarray
) -> List[Dict[str, Any]]:
    """
    Cria pares de produtos baseado na similaridade calculada.
    
    Args:
        df_magalu: DataFrame do Magalu
        df_bemol: DataFrame da Bemol
        matched_indices: Índices dos melhores matches
        matched_scores: Scores de similaridade
        
    Returns:
        List[Dict[str, Any]]: Lista de pares de produtos
    """
    try:
        result = []
        
        for idx, (magalu_row, match_idx, score) in enumerate(
            zip(df_magalu.itertuples(), matched_indices, matched_scores)
        ):
            bemol_row = df_bemol.iloc[match_idx]
            
            # Constrói URLs completas
            magalu_url = construir_url_completa(magalu_row.url, "https://www.magazineluiza.com.br")
            bemol_url = construir_url_completa(bemol_row.url, "https://www.bemol.com.br")
            
            # Adiciona produto do Magalu
            result.append({
                "title": magalu_row.title,
                "marketplace": "Magalu",
                "price": magalu_row.price,
                "url": magalu_url,
                "exclusividade": "não",
                "similaridade": score
            })
            
            # Adiciona produto da Bemol
            result.append({
                "title": bemol_row.title,
                "marketplace": "Bemol",
                "price": bemol_row.price,
                "url": bemol_url,
                "exclusividade": "não",
                "similaridade": score
            })
        
        logger.info(f"Pares de produtos criados: {len(result)} produtos")
        return result
        
    except Exception as e:
        logger.error(f"Erro ao criar pares de produtos: {e}")
        raise


def construir_url_completa(url: str, base_url: str) -> str:
    """
    Constrói URL completa baseada na URL base.
    
    Args:
        url: URL relativa ou absoluta
        base_url: URL base do marketplace
        
    Returns:
        str: URL completa
    """
    if not str(url).startswith("http"):
        return f"{base_url}{url}"
    return url


def processar_embeddings_completos(
    df_magalu: pd.DataFrame,
    df_bemol: pd.DataFrame
) -> pd.DataFrame:
    """
    Processa embeddings completos e retorna DataFrame final ordenado.
    
    Args:
        df_magalu: DataFrame do Magalu com embeddings
        df_bemol: DataFrame da Bemol com embeddings
        
    Returns:
        pd.DataFrame: DataFrame final com todos os produtos ordenados
    """
    try:
        from .data_processing import (
            preparar_dataframe_embeddings,
            identificar_produtos_exclusivos,
            limpar_e_preparar_dataframe_final
        )
        
        # Prepara DataFrames
        df_magalu_prep = preparar_dataframe_embeddings(df_magalu, "Magalu")
        df_bemol_prep = preparar_dataframe_embeddings(df_bemol, "Bemol")
        
        # Calcula similaridade
        sim_matrix, matched_indices, matched_scores = calcular_similaridade_embeddings(
            df_magalu_prep, df_bemol_prep
        )
        
        # Cria pares de produtos
        result = criar_pares_produtos(
            df_magalu_prep, df_bemol_prep, matched_indices, matched_scores
        )
        
        # Identifica produtos exclusivos
        result = identificar_produtos_exclusivos(df_magalu_prep, df_bemol_prep, result)
        
        # Cria DataFrame final
        df_final = pd.DataFrame(result)
        
        # Aplica limpeza completa (remove duplicados, ordena, classifica, calcula diferenças)
        df_final = limpar_e_preparar_dataframe_final(df_final)
        
        logger.info(f"Processamento completo: {len(df_final)} produtos processados")
        
        return df_final
        
    except Exception as e:
        logger.error(f"Erro no processamento de embeddings: {e}")
        raise