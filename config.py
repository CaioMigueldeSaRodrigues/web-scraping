"""
Arquivo de configuração central do projeto.
Nunca coloque segredos diretamente aqui! Use dbutils.secrets.get() no Databricks ou variáveis de ambiente.
"""

# Exemplo de configuração
DATABRICKS_HOST = None  # Defina via variável de ambiente ou notebook
DATABRICKS_TOKEN = None
WAREHOUSE_HTTP_PATH = None

# Parâmetros de scraping
SCRAPING_USER_AGENT = "Mozilla/5.0"
SCRAPING_TIMEOUT = 10

# Caminhos padrão
DATA_DIR = "data/"
REPORTS_DIR = "reports/" 