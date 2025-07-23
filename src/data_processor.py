import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- Constantes ---
EMBEDDING_MODEL = 'neuralmind/bert-base-portuguese-cased'
SIMILARITY_THRESHOLD = 0.85 # Limiar para considerar um "match".

def load_data_from_spark(spark: SparkSession, table_name: str, title_col: str, price_col: str, url_col: str, source_name: str) -> pd.DataFrame:
    """
    Carrega dados de uma tabela Spark, seleciona e renomeia colunas, e converte para Pandas.
    """
    print(f"Carregando dados de: {table_name}")
    df_spark = spark.table(table_name)
    
    df_pandas = df_spark.select(
        col(title_col).alias(f"title_{source_name}"),
        col(price_col).alias(f"price_{source_name}"),
        col(url_col).alias(f"url_{source_name}")
    ).toPandas()
    
    print(f"Carregados {len(df_pandas)} registros de {source_name}.")
    return df_pandas.dropna(subset=[f"title_{source_name}"])

def generate_embeddings(df: pd.DataFrame, text_column: str) -> np.ndarray:
    """
    Gera embeddings para uma coluna de texto de um DataFrame.
    """
    print(f"Gerando embeddings para a coluna '{text_column}'...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    texts = df[text_column].astype(str).tolist()
    
    return model.encode(texts, show_progress_bar=True, batch_size=64)

def find_best_matches(df_marketplace: pd.DataFrame, df_internal: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a similaridade de cosseno, encontra os melhores pares e estrutura o resultado.
    """
    if df_marketplace.empty:
        print("AVISO: DataFrame do marketplace está vazio.")
        return pd.DataFrame()
        
    marketplace_embeddings = generate_embeddings(df_marketplace, 'title_marketplace')
    
    if df_internal.empty:
        print("AVISO: DataFrame interno está vazio. Todos os produtos do marketplace serão marcados como exclusivos.")
        df_marketplace['title_internal'] = None
        df_marketplace['price_internal'] = np.nan
        df_marketplace['url_internal'] = None
        df_marketplace['similarity'] = 0.0
        df_marketplace['exclusividade'] = 'Sim'
        df_marketplace['diferenca_percentual'] = 0.0
        return df_marketplace

    internal_embeddings = generate_embeddings(df_internal, 'title_internal')

    print("Calculando matriz de similaridade de cosseno...")
    similarity_matrix = cosine_similarity(marketplace_embeddings, internal_embeddings)

    results = []
    
    print("Encontrando melhores correspondências...")
    for i in range(len(df_marketplace)):
        best_match_idx = similarity_matrix[i].argmax()
        similarity_score = similarity_matrix[i][best_match_idx]

        match_data = df_marketplace.iloc[i].to_dict()

        if similarity_score >= SIMILARITY_THRESHOLD:
            internal_product = df_internal.iloc[best_match_idx]
            match_data.update(internal_product.to_dict())
            match_data['similarity'] = similarity_score
            match_data['exclusividade'] = 'Não'
        else:
            match_data['title_internal'] = None
            match_data['price_internal'] = np.nan
            match_data['url_internal'] = None
            match_data['similarity'] = similarity_score
            match_data['exclusividade'] = 'Sim'
        
        results.append(match_data)
        
    final_df = pd.DataFrame(results)

    final_df['diferenca_percentual'] = (
        (final_df['price_marketplace'] - final_df['price_internal']) / final_df['price_internal']
    ).fillna(0).replace([np.inf, -np.inf], 0) * 100
    
    column_order = [
        'title_marketplace', 'price_marketplace', 'url_marketplace',
        'title_internal', 'price_internal', 'url_internal',
        'similarity', 'diferenca_percentual', 'exclusividade'
    ]
    final_df = final_df.rename(columns={
        'title_marketplace': 'title', 
        'price_marketplace': 'price', 
        'url_marketplace': 'url'
    })
    
    return final_df.sort_values(by='similarity', ascending=False).reset_index(drop=True) 