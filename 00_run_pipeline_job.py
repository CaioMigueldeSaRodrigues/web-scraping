# Databricks notebook source

# COMMAND ----------

import sys
import os

# --- Bloco de Gerenciamento de Path (Solução Definitiva) ---
# Define o caminho absoluto para a raiz do projeto.
# Este é o método robusto para notebooks executados a partir do Workspace.
project_root = "/Users/caiomiguel@bemol.com.br/web-scraping"

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# COMMAND ----------

from src.main import run_pipeline

run_pipeline() 