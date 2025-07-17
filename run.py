import sys
import os

# Adiciona o diretório raiz do repositório ao path do sistema
# Isso é crucial para que o Python encontre o pacote 'src'
repo_path = os.path.dirname(os.getcwd())
if repo_path not in sys.path:
    sys.path.append(repo_path)

# Agora, importa e executa a função principal do projeto
from src.main import run_pipeline

run_pipeline() 