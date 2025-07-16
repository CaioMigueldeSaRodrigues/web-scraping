# COMMAND ----------
# DBTITLE 0,Install pydantic-settings
# MAGIC %pip install pydantic-settings

# COMMAND ----------
# DBTITLE 1,Install Project Dependencies
# MAGIC %pip install requests beautifulsoup4

# COMMAND ----------
# DBTITLE 2,Setup Environment and Parameters
import sys
import os
from pyspark.sql import SparkSession
import pandas as pd

project_root = os.path.abspath(os.path.join(os.getcwd(), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.config.settings import settings
from src.logger import get_logger
from src.scraping.scraper import Scraper

dbutils.widgets.text("max_pages_per_category", "5", "Max pages to scrape per category")
max_pages = int(dbutils.widgets.get("max_pages_per_category"))

logger = get_logger("OrchestrationNotebook")
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# DBTITLE 3,Execute Scraping Across All Categories
logger.info("Starting scraping pipeline for all configured categories.")
scraper = Scraper(user_agent=settings.USER_AGENT)
all_products_data = []

for category_name, base_url in settings.CATEGORIES.items():
    logger.info(f"--- Processing Category: {category_name} ---")
    products = scraper.scrape_category(base_url=base_url, max_pages=max_pages)
    
    # Adiciona a informação da categoria a cada produto
    for product in products:
        product['category'] = category_name
        
    all_products_data.extend(products)
    logger.info(f"Found {len(products)} products in '{category_name}'. Total so far: {len(all_products_data)}.")

# COMMAND ----------
# DBTITLE 4,Process and Save Data to a Single Delta Table
if all_products_data:
    logger.info(f"Processing a total of {len(all_products_data)} products.")
    
    pdf = pd.DataFrame(all_products_data)
    spark_df = spark.createDataFrame(pdf)
    
    table_path = settings.DELTA_TABLE_PATH
    logger.info(f"Writing data to Delta table: {table_path}")
    
    # Escreve na tabela única, particionando por categoria para otimizar consultas
    spark_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("category") \
        .saveAsTable(table_path)
    
    logger.info("Data successfully saved.")
    display(spark_df)
else:
    logger.warning("No products were scraped across all categories.")

# COMMAND ----------
# DBTITLE 5,Finalize
logger.info("Pipeline finished successfully.")
dbutils.notebook.exit("Success") 