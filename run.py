import sys
import os

# --- Bloco de Gerenciamento de Path (CORRIGIDO) ---
# Usa os.getcwd() que, no contexto de um Job, aponta para a raiz do repositório.
# Isso garante que o pacote 'src' possa ser importado.
project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# ------------------------------------

from src.main import run_pipeline

if __name__ == "__main__":
    print(f"Iniciando a execução do pipeline a partir de run.py. CWD: {project_root}")
    run_pipeline()
    print("Execução do pipeline concluída.") 