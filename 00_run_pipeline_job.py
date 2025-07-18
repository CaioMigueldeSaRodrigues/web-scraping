# Databricks notebook source

# COMMAND ----------

import sys
import os

# --- Bloco de Gerenciamento de Path (SIMPLIFICADO E CORRIGIDO) ---
# No contexto de um notebook executado a partir de um Databricks Repo,
# os.getcwd() retorna a raiz do repositório. Este é o método mais confiável.
project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

from src.main import run_pipeline

run_pipeline() 