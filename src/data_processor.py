# Módulo de processamento de dados, embeddings e comparação
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

EMBEDDING_MODEL = 'neuralmind/bert-base-portuguese-cased'
SIMILARITY_THRESHOLD = 0.85

def load_spark_table_to_pandas(spark: SparkSession, table_name: str, alias: str) -> pd.DataFrame:
    """Carrega uma tabela Spark, renomeia colunas com um alias e converte para Pandas."""
    print(f"Carregando dados de: {table_name}")
    df_spark = spark.table(table_name)
    
    # Padroniza nomes de colunas para processamento
    df_pandas = df_spark.select(
        col("title").alias(f"title_{alias}"),
        col("price").alias(f"price_{alias}"),
        col("url").alias(f"url_{alias}")
    ).toPandas()
    
    print(f"Carregados {len(df_pandas)} registros de {table_name}.")
    return df_pandas.dropna(subset=[f"title_{alias}"])

def generate_embeddings(df: pd.DataFrame, text_column: str) -> np.ndarray:
    """Gera embeddings para uma coluna de texto."""
    print(f"Gerando embeddings para '{text_column}'...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    texts = df[text_column].astype(str).tolist()
    return model.encode(texts, show_progress_bar=True, batch_size=64)

def find_best_matches(df_source: pd.DataFrame, df_target: pd.DataFrame) -> pd.DataFrame:
    """Compara dois DataFrames, encontra os melhores pares e calcula métricas."""
    if df_source.empty:
        print("AVISO: DataFrame de origem (source) está vazio. Retornando DataFrame vazio.")
        return pd.DataFrame()
        
    source_embeddings = generate_embeddings(df_source, 'title_source')
    
    if df_target.empty:
        print("AVISO: DataFrame de destino (target) está vazio. Todos os produtos de origem serão marcados como exclusivos.")
        df_source = df_source.rename(columns={'title_source': 'title', 'price_source': 'price', 'url_source': 'url'})
        df_source['marketplace'] = 'Marketplace' # Placeholder
        df_source['exclusividade'] = 'Sim'
        df_source['diferenca_percentual'] = 0.0
        df_source['similaridade'] = 0.0
        return df_source

    target_embeddings = generate_embeddings(df_target, 'title_target')

    print("Calculando matriz de similaridade...")
    similarity_matrix = cosine_similarity(source_embeddings, target_embeddings)

    results = []
    for i in range(len(df_source)):
        best_match_idx = similarity_matrix[i].argmax()
        similarity_score = similarity_matrix[i][best_match_idx]
        match_data = df_source.iloc[i].to_dict()

        if similarity_score >= SIMILARITY_THRESHOLD:
            match_data.update(df_target.iloc[best_match_idx].to_dict())
            match_data['similaridade'] = similarity_score
            match_data['exclusividade'] = 'Não'
        else:
            match_data.update({
                'title_target': None, 'price_target': np.nan, 'url_target': None,
                'similaridade': similarity_score, 'exclusividade': 'Sim'
            })
        results.append(match_data)
        
    final_df = pd.DataFrame(results)
    final_df['diferenca_percentual'] = ((final_df['price_source'] - final_df['price_target']) / final_df['price_target']).fillna(0).replace([np.inf, -np.inf], 0) * 100
    
    # Renomeia colunas para o formato final da planilha
    final_df = final_df.rename(columns={'title_source': 'title', 'price_source': 'price', 'url_source': 'url'})
    final_df['marketplace'] = final_df.apply(lambda row: 'Bemol' if pd.notna(row['title_target']) else 'Magalu', axis=1)

    return final_df.sort_values(by=['title', 'price'], ascending=[True, True]).reset_index(drop=True) 