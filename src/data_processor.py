import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import col

# --- Constantes ---
# Usar um modelo otimizado para semântica em português é crucial.
EMBEDDING_MODEL = 'neuralmind/bert-base-portuguese-cased'
SIMILARITY_THRESHOLD = 0.85 # Limiar para considerar um "match". Ajustável.

def load_data_from_spark(spark: SparkSession, table_name: str, title_col: str, price_col: str, url_col: str, source_name: str) -> pd.DataFrame:
    """
    Carrega dados de uma tabela Spark, seleciona e renomeia colunas, e converte para Pandas.
    O sufixo '_source' é adicionado para evitar conflitos de nome de coluna após o join.
    """
    print(f"Carregando dados de: {table_name}")
    df_spark = spark.table(table_name)
    
    # Seleciona e renomeia as colunas para um formato padronizado
    df_pandas = df_spark.select(
        col(title_col).alias(f"title_{source_name}"),
        col(price_col).alias(f"price_{source_name}"),
        col(url_col).alias(f"url_{source_name}")
    ).toPandas()
    
    print(f"Carregados {len(df_pandas)} registros de {source_name}.")
    return df_pandas

def generate_embeddings(df: pd.DataFrame, text_column: str) -> np.ndarray:
    """
    Gera embeddings para uma coluna de texto de um DataFrame usando SentenceTransformer.
    """
    print(f"Gerando embeddings para a coluna '{text_column}'...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    # Garante que todos os dados na coluna são strings
    texts = df[text_column].fillna('').astype(str).tolist()
    
    embeddings = model.encode(texts, show_progress_bar=True, batch_size=32)
    return embeddings

def find_best_matches(df_marketplace: pd.DataFrame, df_internal: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a similaridade de cosseno, encontra os melhores pares e identifica produtos exclusivos.
    
    Retorna um DataFrame com a estrutura final para a camada Silver.
    """
    if df_marketplace.empty:
        print("AVISO: DataFrame do marketplace está vazio. Nenhum processamento será feito.")
        return pd.DataFrame()
        
    # Gera embeddings para ambos os dataframes
    marketplace_embeddings = generate_embeddings(df_marketplace, 'title_marketplace')
    
    # Se o dataframe interno estiver vazio, todos os produtos do marketplace são exclusivos
    if df_internal.empty:
        print("AVISO: DataFrame interno está vazio. Todos os produtos do marketplace serão marcados como exclusivos.")
        df_marketplace['title_internal'] = None
        df_marketplace['price_internal'] = np.nan
        df_marketplace['url_internal'] = None
        df_marketplace['similarity'] = 0.0
        return df_marketplace

    internal_embeddings = generate_embeddings(df_internal, 'title_internal')

    print("Calculando matriz de similaridade de cosseno...")
    similarity_matrix = cosine_similarity(marketplace_embeddings, internal_embeddings)

    results = []
    
    print("Encontrando melhores correspondências para cada produto do marketplace...")
    for i in range(len(df_marketplace)):
        # Encontra o índice e o score do produto interno mais similar
        best_match_idx = similarity_matrix[i].argmax()
        similarity_score = similarity_matrix[i][best_match_idx]

        match_data = df_marketplace.iloc[i].to_dict()

        if similarity_score >= SIMILARITY_THRESHOLD:
            # É um match: adiciona dados do produto interno
            internal_product = df_internal.iloc[best_match_idx]
            match_data.update(internal_product.to_dict())
            match_data['similarity'] = similarity_score
            match_data['exclusividade'] = 'Não'
        else:
            # Não é um match (exclusivo): preenche com nulos
            match_data['title_internal'] = None
            match_data['price_internal'] = np.nan
            match_data['url_internal'] = None
            match_data['similarity'] = similarity_score # Mantém o score do mais próximo, mesmo que baixo
            match_data['exclusividade'] = 'Sim'
        
        results.append(match_data)
        
    final_df = pd.DataFrame(results)

    # Calcula o diferencial de preço percentual
    # (Preço Marketplace - Preço Interno) / Preço Interno
    final_df['diferenca_percentual'] = (
        (final_df['price_marketplace'] - final_df['price_internal']) / final_df['price_internal']
    ).fillna(0) * 100
    
    # Reordena as colunas para clareza
    column_order = [
        'title_marketplace', 'price_marketplace', 'url_marketplace',
        'title_internal', 'price_internal', 'url_internal',
        'similarity', 'diferenca_percentual', 'exclusividade'
    ]
    # Filtra para garantir que apenas as colunas desejadas existam
    final_df = final_df[[col for col in column_order if col in final_df.columns]]
    
    return final_df.sort_values(by='similarity', ascending=False).reset_index(drop=True) 