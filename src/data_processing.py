import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import logging
from src.config import EMBEDDING_MODEL, SIMILARITY_THRESHOLD

def generate_embeddings(df: pd.DataFrame, text_column: str) -> pd.DataFrame:
    logging.info(f"Gerando embeddings para a coluna '{text_column}' com '{EMBEDDING_MODEL}'...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    embeddings = model.encode(df[text_column].tolist(), show_progress_bar=True, device='cuda') # Use 'cuda' se tiver GPU
    df['embedding'] = list(embeddings)
    return df

def find_similar_products(df_site: pd.DataFrame, df_tabela: pd.DataFrame) -> pd.DataFrame:
    if df_site.empty or df_tabela.empty:
        logging.warning("Um dos DataFrames está vazio. Retornando apenas os produtos exclusivos do site.")
        df_site['exclusivo'] = True
        return df_site[['titulo_site', 'preco_site', 'url_site', 'categoria_site', 'exclusivo']]

    logging.info("Calculando similaridade de cosseno...")
    site_embeddings = np.array(df_site['embedding'].tolist())
    tabela_embeddings = np.array(df_tabela['embedding'].tolist())
    similarity_matrix = cosine_similarity(site_embeddings, tabela_embeddings)

    results = []
    matched_site_indices = set()

    for i in range(len(df_site)):
        best_match_idx = np.argmax(similarity_matrix[i])
        similarity_score = similarity_matrix[i][best_match_idx]

        if similarity_score >= SIMILARITY_THRESHOLD:
            site_product = df_site.iloc[i]
            tabela_product = df_tabela.iloc[best_match_idx]
            price_diff = ((site_product['preco_site'] - tabela_product['preco_tabela']) / tabela_product['preco_tabela']) * 100
            
            results.append({
                'produto_site': site_product['titulo_site'],
                'produto_tabela': tabela_product['titulo_tabela'],
                'similaridade': similarity_score,
                'preco_site': site_product['preco_site'],
                'preco_tabela': tabela_product['preco_tabela'],
                'diferencial_percentual': price_diff,
                'url_site': site_product['url_site'],
                'categoria_site': site_product['categoria_site'],
                'id_tabela': tabela_product['id_tabela'],
                'exclusivo': False
            })
            matched_site_indices.add(i)

    exclusive_site_indices = set(range(len(df_site))) - matched_site_indices
    for i in exclusive_site_indices:
        site_product = df_site.iloc[i]
        results.append({
            'produto_site': site_product['titulo_site'],
            'produto_tabela': None, 'similaridade': 0, 'preco_site': site_product['preco_site'],
            'preco_tabela': None, 'diferencial_percentual': None, 'url_site': site_product['url_site'],
            'categoria_site': site_product['categoria_site'], 'id_tabela': None, 'exclusivo': True
        })

    final_df = pd.DataFrame(results)
    return final_df.sort_values(by=['exclusivo', 'diferencial_percentual'], ascending=[True, False]) 