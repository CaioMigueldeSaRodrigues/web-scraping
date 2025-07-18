# Databricks notebook source

# COMMAND ----------

import sys
import os

# --- Bloco de Gerenciamento de Path (Solução Final) ---
# No contexto de um notebook executado a partir do Workspace,
# os.getcwd() retorna a raiz do diretório do projeto.
project_root = os.getcwd()

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

from src.main import run_pipeline

run_pipeline() 