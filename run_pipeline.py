# CÉLULA ÚNICA DO NOTEBOOK 'run_pipeline.py' - VERSÃO CORRIGIDA

# 1. Instalação de dependências
print("Passo 1: Instalando dependências...")
%pip install -r requirements.txt
print("Dependências instaladas.")
print("---")

# 2. Configuração EXPLÍCITA do path
import sys
import os

# O Databricks Repos clona o projeto em um caminho específico. 
# O comando '%pwd' no Databricks nos dá o caminho do notebook atual.
# Precisamos subir um nível para chegar à raiz do projeto.
notebook_path = os.getcwd()
project_root = os.path.dirname(notebook_path)

# Adiciona a raiz do projeto ao Python Path para encontrar 'main.py'
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Passo 2: Camin")
