# Databricks notebook source

import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import logging
from pyspark.sql import DataFrame

def _generate_embeddings(df: pd.DataFrame, text_column: str, model_name: str) -> pd.DataFrame:
    model = SentenceTransformer(model_name)
    embeddings = model.encode(df[text_column].tolist(), show_progress_bar=True, device='cpu')
    df['embedding'] = list(embeddings)
    return df

# Função principal do módulo
def execute_processing(df_site_spark: DataFrame, df_tabela_spark: DataFrame, config: dict) -> (pd.DataFrame, pd.DataFrame):
    """Orquestra a geração de embeddings e o pareamento, retornando os relatórios."""
    logging.info("--- INICIANDO ETAPA DE PROCESSAMENTO E PAREAMENTO ---")
    
    # 1. Geração de Embeddings
    df_site_pandas = df_site_spark.toPandas()
    df_tabela_pandas = df_tabela_spark.toPandas()
    
    df_site_embedded = _generate_embeddings(df_site_pandas, 'titulo_site', config['EMBEDDING_MODEL'])
    df_tabela_embedded = _generate_embeddings(df_tabela_pandas, 'titulo_tabela', config['EMBEDDING_MODEL'])

    # 2. Cálculo de Similaridade e Pareamento
    site_embeddings = np.array(df_site_embedded['embedding'].tolist())
    tabela_embeddings = np.array(df_tabela_embedded['embedding'].tolist())
    similarity_matrix = cosine_similarity(site_embeddings, tabela_embeddings)

    results = []
    for i in range(len(df_site_embedded)):
        best_match_idx = np.argmax(similarity_matrix[i])
        similarity_score = similarity_matrix[i][best_match_idx]
        site_product = df_site_embedded.iloc[i]
        tabela_product = df_tabela_embedded.iloc[best_match_idx]

        is_match = similarity_score >= config['SIMILARITY_THRESHOLD']
        price_diff = ((site_product['preco_site'] - tabela_product['preco_tabela']) / tabela_product['preco_tabela']) * 100 if is_match and tabela_product['preco_tabela'] else None
        
        results.append({
            'produto_site': site_product['titulo_site'],
            'produto_tabela': tabela_product['titulo_tabela'] if is_match else None,
            'similaridade': similarity_score,
            'preco_site': site_product['preco_site'],
            'preco_tabela': tabela_product['preco_tabela'] if is_match else None,
            'diferencial_percentual': price_diff,
            'url_site': site_product['url_site'],
            'exclusivo': not is_match
        })
    
    analytical_report_df = pd.DataFrame(results)

    # 3. Formatação do Relatório de Negócios
    business_rows = []
    for _, row in analytical_report_df.iterrows():
        if not row['exclusivo']:
            business_rows.extend([
                {'title': row['produto_site'], 'marketplace': 'Bemol', 'price': row['preco_tabela'], 'url': '', 'exclusividade': 'Não', 'diferenca_percentual': f"{row['diferencial_percentual']:.2f}%"},
                {'title': row['produto_site'], 'marketplace': 'Magalu', 'price': row['preco_site'], 'url': row['url_site'], 'exclusividade': 'Não', 'diferenca_percentual': f"{row['diferencial_percentual']:.2f}%"}
            ])
        else:
             business_rows.append({'title': row['produto_site'], 'marketplace': 'Magalu', 'price': row['preco_site'], 'url': row['url_site'], 'exclusividade': 'Sim', 'diferenca_percentual': 'N/A'})
    
    business_report_df = pd.DataFrame(business_rows)
    
    logging.info("Processamento concluído.")
    return analytical_report_df, business_report_df 