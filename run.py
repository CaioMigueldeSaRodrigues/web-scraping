import sys
import os

# --- Bloco de Gerenciamento de Path ---
# Adiciona o diretório raiz do projeto ao path do Python.
# Isso garante que o pacote 'src' possa ser importado.
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# ------------------------------------

from src.main import run_pipeline

if __name__ == "__main__":
    print("Iniciando a execução do pipeline a partir de run.py...")
    run_pipeline()
    print("Execução do pipeline concluída.") 