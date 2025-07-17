import requests
from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging
import time
import re
from src.config import MAGALU_CATEGORIES, MAX_PAGES_PER_CATEGORY, DATABRICKS_TABLE, BRONZE_LAYER_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            logging.info(f"[{category_name}] Extraindo página {page}...")
            soup = BeautifulSoup(response.content, 'html.parser')
            cards = soup.select('div[data-testid="product-card-content"]')
            if not cards:
                logging.warning(f"[{category_name}] Nenhum produto na página {page}. Encerrando categoria.")
                break
            for card in cards:
                title_tag = card.select_one('h2[data-testid="product-title"]')
                price_tag = card.select_one('p[data-testid="price-value"]')
                link_tag = card.find_parent('a')
                if title_tag and link_tag:
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

def scrape_and_save_all_categories(spark: SparkSession):
    logging.info("--- INICIANDO SCRAPING DE TODAS AS CATEGORIAS ---")
    for category_name, base_url in MAGALU_CATEGORIES.items():
        products = _scrape_category_page(category_name, base_url, MAX_PAGES_PER_CATEGORY)
        if products:
            spark_df = spark.createDataFrame(pd.DataFrame(products))
            spark_df = spark_df.withColumn("preco_site", col("preco_site").cast("double")).filter(col("preco_site").isNotNull())
            table_name = BRONZE_LAYER_PATH.format(category_name.lower())
            logging.info(f"Salvando {spark_df.count()} produtos de '{category_name}' em '{table_name}'...")
            spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        else:
            logging.warning(f"[!] Nenhum produto extraído para '{category_name}'.")

def load_scraped_data(spark: SparkSession) -> DataFrame:
    logging.info("--- CARREGANDO E UNIFICANDO DADOS DA CAMADA BRONZE ---")
    all_dfs = []
    for category_name in MAGALU_CATEGORIES.keys():
        table_name = BRONZE_LAYER_PATH.format(category_name.lower())
        try:
            all_dfs.append(spark.read.table(table_name))
        except Exception as e:
            logging.warning(f"Tabela '{table_name}' não encontrada. Erro: {e}")
    if not all_dfs:
        raise RuntimeError("Nenhuma tabela da camada bronze foi carregada. Pipeline não pode continuar.")
    
    unified_df = all_dfs[0]
    for i in range(1, len(all_dfs)):
        unified_df = unified_df.unionByName(all_dfs[i])
    logging.info(f"Total de {unified_df.count()} produtos unificados de {len(all_dfs)} categorias.")
    return unified_df

def load_databricks_table(spark: SparkSession) -> DataFrame:
    logging.info(f"Carregando tabela interna {DATABRICKS_TABLE}...")
    df = spark.sql(f"SELECT id as id_tabela, titulo as titulo_tabela, preco as preco_tabela, link as url_tabela FROM {DATABRICKS_TABLE} WHERE preco IS NOT NULL AND titulo IS NOT NULL")
    logging.info(f"{df.count()} registros carregados da tabela {DATABRICKS_TABLE}.")
    return df 