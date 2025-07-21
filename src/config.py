import os
import logging
from pyspark.sql import SparkSession

def get_dbutils():
    try:
        from pyspark.dbutils import DBUtils
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except (ImportError, ModuleNotFoundError):
        return None

def get_secret(scope: str, key: str) -> str | None:
    dbutils = get_dbutils()
    if dbutils:
        try:
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            logging.error(f"Falha ao buscar segredo '{key}' do escopo '{scope}'.")
            return None
    else:
        env_var = f"DB_{scope.upper().replace('-', '_')}_{key.upper().replace('-', '_')}"
        return os.getenv(env_var)

SECRET_SCOPE = "bemol-data-secrets"
SENDGRID_API_KEY_NAME = "SendGridAPI"

MAX_PAGES_PER_CATEGORY = 17
MAGALU_CATEGORIES = {
    "Eletroportateis": "https://www.magazineluiza.com.br/eletroportateis/l/ep/?page={}",
    "Informatica": "https://www.magazineluiza.com.br/informatica/l/in/?page={}",
    "Tv_e_Video": "https://www.magazineluiza.com.br/tv-e-video/l/et/?page={}",
    "Moveis": "https://www.magazineluiza.com.br/moveis/l/mo/?page={}",
    "Eletrodomesticos": "https://www.magazineluiza.com.br/eletrodomesticos/l/ed/?page={}",
    "Celulares": "https://www.magazineluiza.com.br/celulares-e-smartphones/l/te/?page={}"
}

BRONZE_LAYER_PATH = "bronze.magalu_{}"
DATABRICKS_TABLE = "bol.feed_varejo_vtex"

EMBEDDING_MODEL = 'paraphrase-multilingual-mpnet-base-v2'
# --- AJUSTE DE THRESHOLD ---
# Limiar de similaridade para considerar dois produtos como um "match".
# Valores mais baixos (ex: 0.75) encontrarão mais pares, mas com menor precisão.
# Valores mais altos (ex: 0.90) encontrarão menos pares, mas com maior precisão.
SIMILARITY_THRESHOLD = 0.80 # Reduzido de 0.85 para 0.80 para aumentar a captura de pares.

SENDGRID_API_KEY = get_secret(scope=SECRET_SCOPE, key=SENDGRID_API_KEY_NAME)
FROM_EMAIL = "caiomiguel@bemol.com.br"
TO_EMAILS = ["gabrielbarros@bemol.com.br"]
EMAIL_SUBJECT = "Análise de Concorrência de Preços - Magazine Luiza" 