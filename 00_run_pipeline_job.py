# Databricks notebook source

# COMMAND ----------

import sys
import os
import logging

# --- Bloco de Gerenciamento de Path ---
# Usa os.getcwd() que, no contexto de um notebook executado a partir de um Repo,
# aponta para a raiz desse repositório. Isso garante que o pacote 'src' seja encontrado.
project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

# Importa a função principal do seu pacote Python
from src.main import run_pipeline

print(f"Iniciando a execução do pipeline a partir do wrapper notebook. CWD: {project_root}")
run_pipeline()
print("Execução do pipeline concluída.") 