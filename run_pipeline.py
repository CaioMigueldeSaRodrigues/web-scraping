# CÉLULA ÚNICA DO NOTEBOOK 'run_pipeline.py'

# 1. Instalação de dependências a partir do seu arquivo requirements.txt
print("Passo 1: Instalando dependências...")
# ATENÇÃO: Execute a linha abaixo apenas em notebooks (ex: Databricks, Jupyter). Comente se rodar como script Python.
# %pip install -r requirements.txt
print("Dependências instaladas.")
print("---")

# 2. Configuração do path para que o Python encontre seus módulos (src, etc.)
import sys
import os
project_root = os.path.abspath(os.getcwd())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Passo 2: Project Root adicionado ao Python Path: {project_root}")
print("---")

# 3. Importação e execução do ponto de entrada da aplicação (main.py)
try:
    from main import main
    print("Passo 3: Iniciando a execução da função main()...")
    
    # Esta linha executa todo o seu pipeline
    main()
    
    print("---")
    print("Pipeline concluído com sucesso.")
    
except ImportError as e:
    print(f"ERRO DE IMPORTAÇÃO: O módulo não foi encontrado. Verifique a estrutura de pastas e o sys.path.")
    print(f"Detalhes: {e}")
    raise e
except Exception as e:
    print(f"ERRO NA EXECUÇÃO DO PIPELINE.")
    print(f"Detalhes: {e}")
    # Levanta a exceção para que o Databricks Jobs marque a execução como FALHA.
    raise e
