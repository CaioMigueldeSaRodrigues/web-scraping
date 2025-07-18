# Databricks notebook source

# COMMAND ----------

# Configuração robusta de paths para Databricks
import sys
import os
from pathlib import Path

# Adicionar o diretório raiz do projeto ao PYTHONPATH
project_root = Path("/Workspace/Repos/web-scraping-main")
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Verificar se estamos no ambiente Databricks
try:
    dbutils
    print("✅ Executando no Databricks")
except NameError:
    print("⚠️ Executando fora do Databricks - usando paths locais")
    # Para desenvolvimento local
    current_dir = Path.cwd()
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))

# COMMAND ----------

# Importar e executar o pipeline
try:
    from src.main import run_pipeline
    print("✅ Módulo src.main importado com sucesso")
    
    # Executar o pipeline
    run_pipeline()
    
except ImportError as e:
    print(f"❌ Erro de importação: {e}")
    print("📁 Conteúdo do diretório atual:")
    import subprocess
    result = subprocess.run(['ls', '-la'], capture_output=True, text=True)
    print(result.stdout)
    
    print("\n📁 Conteúdo do diretório src:")
    result = subprocess.run(['ls', '-la', 'src'], capture_output=True, text=True)
    print(result.stdout)
    
except Exception as e:
    print(f"❌ Erro durante execução: {e}")
    import traceback
    traceback.print_exc() 