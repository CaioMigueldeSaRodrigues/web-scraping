import os
import logging
from pyspark.sql import SparkSession
from typing import Dict, Any

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
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception as e:
            logging.error(f"Falha ao buscar segredo '{key}' do escopo '{scope}'. Detalhes: {e}")
            return None
    else:
        env_var = f"DB_{scope.upper().replace('-', '_')}_{key.upper().replace('-', '_')}"
        logging.warning(f"Buscando segredo em variável de ambiente: {env_var}")
        return os.getenv(env_var)

# --- Configurações de Segredos ---
SECRET_SCOPE = "bemol-data-secrets"
SENDGRID_API_KEY_NAME = "SendGridAPI"

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
# Configurações do modelo de embeddings
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
BATCH_SIZE = 250

# Configurações de similaridade
SIMILARITY_THRESHOLD = 0.85
SIMILARITY_HIGH = 0.95
SIMILARITY_MODERATE = 0.85
SIMILARITY_LOW = 0.5

# Configurações de URLs dos marketplaces
MARKETPLACE_URLS = {
    "Magalu": "https://www.magazineluiza.com.br",
    "Bemol": "https://www.bemol.com.br"
}

# Configurações de tabelas
DEFAULT_TABLES = {
    "magalu": "silver.embeddings_magalu_completo",
    "bemol": "silver.embeddings_bemol"
}

# Configurações de relatórios
REPORT_COLUMNS = {
    "visible": ["title", "marketplace", "price", "url", "exclusividade"],
    "hidden": ["similaridade", "nivel_similaridade", "diferenca_percentual"]
}

# Configurações de exportação
DEFAULT_EXCEL_PATH = "benchmarking_produtos.xlsx"
DEFAULT_TEMPVIEW_NAME = "tempview_benchmarking_pares"

# Configurações de logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Configurações de processamento
MAX_PRODUCTS_PER_BATCH = 1000
PRICE_CLEANING_REGEX = r'(\d{1,3}(?:\.\d{3})*,\d{2})'

# Configurações de classificação de similaridade
SIMILARITY_CLASSIFICATION = {
    "exclusivo": -1,
    "muito_similar": 0.85,
    "moderadamente_similar": 0.5,
    "pouco_similar": 0.0
}

# Configurações de diferença percentual
PRICE_DIFFERENCE_THRESHOLD = 0.90  # Apenas produtos com similaridade >= 0.90

# Configurações de validação
VALIDATION_SETTINGS = {
    "min_products": 1,
    "max_products": 100000,
    "required_columns": ["title", "price", "url", "embedding"]
}

# Configurações de performance
PERFORMANCE_SETTINGS = {
    "use_gpu": False,  # Forçar uso de CPU no Databricks
    "memory_limit": "4GB",
    "max_workers": 4
}

# Configurações de segurança
SECURITY_SETTINGS = {
    "mask_sensitive_data": True,
    "log_sensitive_operations": False
}

# Configurações de monitoramento
MONITORING_SETTINGS = {
    "enable_metrics": True,
    "track_performance": True,
    "alert_on_errors": True
}


def get_config() -> Dict[str, Any]:
    """
    Retorna configuração completa do projeto.
    
    Returns:
        Dict[str, Any]: Configuração completa
    """
    return {
        "embedding_model": EMBEDDING_MODEL,
        "batch_size": BATCH_SIZE,
        "similarity_threshold": SIMILARITY_THRESHOLD,
        "marketplace_urls": MARKETPLACE_URLS,
        "default_tables": DEFAULT_TABLES,
        "report_columns": REPORT_COLUMNS,
        "default_excel_path": DEFAULT_EXCEL_PATH,
        "default_tempview_name": DEFAULT_TEMPVIEW_NAME,
        "log_level": LOG_LEVEL,
        "max_products_per_batch": MAX_PRODUCTS_PER_BATCH,
        "price_cleaning_regex": PRICE_CLEANING_REGEX,
        "similarity_classification": SIMILARITY_CLASSIFICATION,
        "price_difference_threshold": PRICE_DIFFERENCE_THRESHOLD,
        "validation_settings": VALIDATION_SETTINGS,
        "performance_settings": PERFORMANCE_SETTINGS,
        "security_settings": SECURITY_SETTINGS,
        "monitoring_settings": MONITORING_SETTINGS
    }


def validate_config() -> bool:
    """
    Valida configurações do projeto.
    
    Returns:
        bool: True se configurações são válidas
    """
    try:
        # Validações básicas
        assert EMBEDDING_MODEL in ["all-MiniLM-L6-v2", "all-MiniLM-L12-v2"], "Modelo de embedding inválido"
        assert 0 < BATCH_SIZE <= 1000, "Batch size deve estar entre 1 e 1000"
        assert 0 <= SIMILARITY_THRESHOLD <= 1, "Threshold de similaridade deve estar entre 0 e 1"
        assert len(MARKETPLACE_URLS) > 0, "Deve haver pelo menos um marketplace configurado"
        
        return True
        
    except AssertionError as e:
        print(f"Erro de validação de configuração: {e}")
        return False 