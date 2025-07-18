# Databricks notebook source

# COMMAND ----------

import sys
import os

# Método robusto para obter o caminho raiz do repositório a partir de um notebook.
# Usa dbutils para encontrar o caminho do próprio notebook e extrai o diretório pai.
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    project_root = os.path.dirname(notebook_path)
except NameError:
    # Fallback para o caso de execução fora de um notebook Databricks
    project_root = os.getcwd()

# Adiciona a raiz ao sys.path se ainda não estiver lá.
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

from src.main import run_pipeline

run_pipeline() 