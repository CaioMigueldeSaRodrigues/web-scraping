# Databricks notebook source

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging
import time
import re

# Funções auxiliares (privadas)
def _clean_price(price_str: str) -> float | None:
    if not price_str or "indisponível" in price_str.lower(): return None
    cleaned_price = re.sub(r'[R$\s.]', '', price_str).replace(',', '.')
    try: return float(cleaned_price)
    except (ValueError, TypeError): return None

def _scrape_category_page(category_name: str, base_url: str, max_pages: int) -> list[dict]:
    products_data = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    for page in range(1, max_pages + 1):
        url = base_url.format(page)
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            for card in BeautifulSoup(response.content, 'html.parser').select('div[data-testid="product-card-content"]'):
                title_tag = card.select_one('h2[data-testid="product-title"]')
                link_tag = card.find_parent('a')
                if title_tag and link_tag:
                    price_tag = card.select_one('p[data-testid="price-value"]')
                    products_data.append({
                        'titulo_site': title_tag.get_text(strip=True),
                        'preco_site': _clean_price(price_tag.get_text(strip=True)) if price_tag else None,
                        'url_site': "https://www.magazineluiza.com.br" + link_tag['href'],
                        'categoria_site': category_name
                    })
        except requests.RequestException as e:
            logging.error(f"[{category_name}] Erro de requisição na página {page}: {e}")
            break
        time.sleep(1)
    return products_data

# Função principal do módulo
def execute_scraping_and_load(spark: SparkSession, config: dict) -> (DataFrame, DataFrame):
    """Orquestra o scraping e o carregamento dos dados, retornando os DataFrames Spark."""
    logging.info("--- INICIANDO ETAPA DE EXTRAÇÃO DE DADOS ---")
    
    # 1. Scraping e salvamento na camada Bronze
    for category_name, base_url in config['MAGALU_CATEGORIES'].items():
        products = _scrape_category_page(category_name, base_url, config['MAX_PAGES_PER_CATEGORY'])
        if products:
            spark_df = spark.createDataFrame(pd.DataFrame(products))
            spark_df = spark_df.withColumn("preco_site", col("preco_site").cast("double")).filter(col("preco_site").isNotNull())
            table_name = config['BRONZE_LAYER_PATH'].format(category_name.lower())
            spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

    # 2. Carregamento e unificação dos dados raspados
    all_dfs = [spark.read.table(config['BRONZE_LAYER_PATH'].format(cat.lower())) for cat in config['MAGALU_CATEGORIES'].keys()]
    df_site_spark = all_dfs[0]
    for i in range(1, len(all_dfs)):
        df_site_spark = df_site_spark.unionByName(all_dfs[i])

    # 3. Carregamento da tabela interna
    query = f"SELECT id as id_tabela, title as titulo_tabela, price as preco_tabela, link as url_tabela FROM {config['DATABRICKS_TABLE']} WHERE price IS NOT NULL AND title IS NOT NULL"
    df_tabela_spark = spark.sql(query)
    
    logging.info(f"Extração concluída. Site: {df_site_spark.count()} produtos. Tabela: {df_tabela_spark.count()} produtos.")
    return df_site_spark, df_tabela_spark 