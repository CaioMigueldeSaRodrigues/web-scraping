# COMMAND ----------
# DBTITLE 1,Setup & Widgets
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import pandas as pd

# Adiciona o diretório 'src' ao path para permitir importações
project_root = os.path.abspath(os.path.join(os.getcwd(), '..'))
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from src.config.settings import settings
from src.logger_config import logger
from src.scraping.scraper import Scraper

# Widgets para controle dinâmico da execução.
dbutils.widgets.text("site_name", "magazine_luiza", "Target site name (e.g., magazine_luiza)")
dbutils.widgets.text("search_term", "iphone 15", "Product Search Term")

# Parâmetros obtidos de widgets.
site_name = dbutils.widgets.get("site_name")
search_term = dbutils.widgets.get("search_term")

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# DBTITLE 2,Execute Scraping
logger.info(f"Starting pipeline for site '{site_name}' with search term: '{search_term}'")

try:
    site_config = settings.SCRAPING_CONFIG[site_name]
except KeyError:
    dbutils.notebook.exit(f"ERROR: Site '{site_name}' not found in configuration.")

search_url = f"{site_config['base_url']}{search_term.replace(' ', '%20')}"

try:
    # A instância do Scraper agora busca o user_agent do arquivo de configuração.
    scraper = Scraper(user_agent=settings.USER_AGENT)
    scraped_data = scraper.scrape(site_name=site_name, url=search_url)
    logger.info("Scraping completed.")

except Exception as e:
    logger.critical(f"A critical error occurred during scraping: {e}", exc_info=True)
    dbutils.notebook.exit(f"Pipeline failed: {e}")

# COMMAND ----------
# DBTITLE 3,Process and Save Data
if scraped_data:
    logger.info(f"Processing {len(scraped_data)} products from {site_name}.")
    pdf = pd.DataFrame(scraped_data)
    sdf = spark.createDataFrame(pdf)
    sdf_final = sdf.withColumn("search_term", lit(search_term)) \
                   .withColumn("scraped_at", current_timestamp())
    table_name = site_config['table_name']
    sdf_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    logger.info(f"Successfully wrote data to Delta table: {table_name}")
    display(sdf_final)
else:
    logger.warning(f"No data returned from {site_name} scraper.")

# COMMAND ----------
logger.info("Pipeline finished successfully.")
dbutils.notebook.exit("Success") 