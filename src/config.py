import os
import logging
from pyspark.sql import SparkSession

def get_dbutils():
    """Retorna a instância do dbutils se estiver em um ambiente Databricks."""
    try:
        from pyspark.dbutils import DBUtils
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except (ImportError, ModuleNotFoundError):
        logging.warning("dbutils não encontrado. Assumindo ambiente de execução local/teste.")
        return None

def get_secret(scope: str, key: str) -> str | None:
    """Obtém um segredo do Databricks Secrets, conforme o padrão especificado."""
    dbutils = get_dbutils()
    if dbutils:
        try:
            logging.info(f"Buscando segredo '{key}' no escopo '{scope}' do Databricks.")
            # --- A CHAMADA CRÍTICA ACONTECE AQUI, USANDO OS PARÂMETROS ---
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            logging.error(f"Falha ao buscar segredo '{key}' do escopo '{scope}'. Verifique se o segredo existe e as permissões estão corretas.")
            return None
    else:
        # Fallback para desenvolvimento local
        env_var = f"DB_{scope.upper().replace('-', '_')}_{key.upper().replace('-', '_')}"
        logging.warning(f"Buscando segredo em variável de ambiente: {env_var}")
        return os.getenv(env_var)

# --- Configurações de Segredos ---
# Define o escopo e a chave a serem usados na chamada da função get_secret
SECRET_SCOPE = "bemol-data-secrets"
SENDGRID_API_KEY_NAME = "sendgrid-api-key"

# --- Scraping de Categorias ---
MAX_PAGES_PER_CATEGORY = 17
MAGALU_CATEGORIES = {
    "Eletroportateis": "https://www.magazineluiza.com.br/eletroportateis/l/ep/?page={}",
    "Informatica": "https://www.magazineluiza.com.br/informatica/l/in/?page={}",
    "Tv_e_Video": "https://www.magazineluiza.com.br/tv-e-video/l/et/?page={}",
    "Moveis": "https://www.magazineluiza.com.br/moveis/l/mo/?page={}",
    "Eletrodomesticos": "https://www.magazineluiza.com.br/eletrodomesticos/l/ed/?page={}",
    "Celulares": "https://www.magazineluiza.com.br/celulares-e-smartphones/l/te/?page={}"
}

# --- Nomes de Tabela Delta ---
BRONZE_LAYER_PATH = "bronze.magalu_{}"
DATABRICKS_TABLE = "bol.feed_varejo_vtex"

# --- Embeddings ---
EMBEDDING_MODEL = 'paraphrase-multilingual-mpnet-base-v2'
SIMILARITY_THRESHOLD = 0.85

# --- Reporting & Email ---
# --- USO DA FUNÇÃO COM O ESCOPO E CHAVE CORRETOS ---
SENDGRID_API_KEY = get_secret(scope=SECRET_SCOPE, key=SENDGRID_API_KEY_NAME)
FROM_EMAIL = "caiomiguel@bemol.com.br"
TO_EMAILS = ["gabrielbarros@bemol.com.br"]
EMAIL_SUBJECT = "Análise de Concorrência de Preços - Magazine Luiza" 