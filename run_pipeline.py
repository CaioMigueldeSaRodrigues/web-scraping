# CÉLULA 1: INSTALAÇÃO DE DEPENDÊNCIAS E EXECUÇÃO DO PIPELINE

# 1. Instalar todas as bibliotecas do seu projeto a partir do requirements.txt
# ATENÇÃO: Execute a linha abaixo apenas em notebooks (ex: Databricks, Jupyter). Comente se rodar como script Python.
# %pip install -r requirements.txt

# 2. Configurar o ambiente para que o Python encontre seus módulos
import sys
import os

# Adiciona o diretório raiz do repositório ao path do sistema.
# Isso permite que 'import main' e os imports dentro de main.py ('from src.module import...') funcionem.
project_root = os.path.abspath(os.getcwd())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Project Root: {project_root}")
print("---")

# 3. Importar e executar o ponto de entrada da sua aplicação
try:
    print("Iniciando a importação do script 'main'...")
    from main import main
    
    print("Executando a função principal do pipeline...")
    main() # Esta é a chamada que executa todo o seu processo.
    
    print("---")
    print("Pipeline concluído com sucesso.")
    
except ImportError as e:
    print(f"ERRO DE IMPORTAÇÃO: Não foi possível encontrar um módulo. Verifique os caminhos e a estrutura do projeto. Detalhes: {e}")
    print("Python Path atual:", sys.path)
except Exception as e:
    print(f"ERRO NA EXECUÇÃO DO PIPELINE: {e}")
    # Levanta a exceção para que o Databricks Jobs marque a execução como falha.
    raise e

# Opcional: Para execuções via Databricks Jobs, isso sinaliza um término bem-sucedido.
try:
    dbutils.notebook.exit("Pipeline execution finished successfully.")
except Exception:
    pass  # Ignora erro se dbutils não estiver disponível
