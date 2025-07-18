# Databricks notebook source

# COMMAND ----------

import sys
import os

# --- Bloco de Gerenciamento de Path (Solução /dbfs/) ---
# Define o caminho absoluto do sistema de arquivos que o cluster enxerga.
project_root = "/dbfs/Users/caiomiguel@bemol.com.br/web-scraping"

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

from src.main import run_pipeline

run_pipeline() 