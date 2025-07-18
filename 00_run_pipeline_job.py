# Databricks notebook source

# COMMAND ----------

import sys
import os

# --- CÉLULA DE DIAGNÓSTICO ---

# 1. Obter o caminho raiz do projeto
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    project_root = os.path.dirname(notebook_path)
except Exception as e:
    print(f"AVISO: Falha ao usar dbutils, usando os.getcwd(). Erro: {e}")
    project_root = os.getcwd()

print(f"--- DIAGNÓSTICO DE PATH ---")
print(f"Caminho do Projeto (project_root): {project_root}")

# 2. Adicionar o caminho ao sys.path
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print("Caminho do projeto inserido no sys.path.")
else:
    print("Caminho do projeto já estava no sys.path.")

print("\nConteúdo do sys.path (primeiros 5 itens):")
print(sys.path[:5])

# 3. Listar o conteúdo da raiz do projeto para verificar se 'src' existe
print(f"\nConteúdo de '{project_root}':")
try:
    print(os.listdir(project_root))
except Exception as e:
    print(f"ERRO ao listar diretório raiz: {e}")

# 4. Listar o conteúdo de 'src' para verificar se '__init__.py' existe
src_path = os.path.join(project_root, 'src')
print(f"\nConteúdo de '{src_path}':")
try:
    print(os.listdir(src_path))
except Exception as e:
    print(f"ERRO ao listar diretório 'src': {e}")

print("--- FIM DO DIAGNÓSTICO ---")

# COMMAND ----------

# --- CÉLULA DE EXECUÇÃO ---

# A execução só continuará se o diagnóstico acima não falhar
from src.main import run_pipeline

run_pipeline() 