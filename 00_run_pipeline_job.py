# Databricks notebook source

# COMMAND ----------
import sys
import os

# Tenta importar dbutils (útil para listar arquivos no DBFS/Workspace Files em Databricks)
try:
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
except (ImportError, NameError): # NameError if SparkSession not available
    dbutils = None

# --- Bloco de Gerenciamento de Path (Usando os.getcwd()) ---
# O os.getcwd() retorna o diretório onde o notebook está sendo executado.
project_root = os.getcwd()

if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"--- Diagnóstico de Path Detalhado em Tempo de Execução ---")
print(f"1. project_root definido no notebook: {project_root}")
print(f"2. Conteúdo atual do sys.path (primeiros 5 itens): {sys.path[:5]}")
print(f"3. Diretório de trabalho atual (cwd): {os.getcwd()}") # Deve ser o mesmo que project_root

# --- Verificações Cruciais da Estrutura de Arquivos ---
# Tentativa de listar o cwd para ver o conteúdo
try:
    print(f"\n4. Conteúdo do diretório de trabalho atual ('{os.getcwd()}'):")
    current_dir_contents = os.listdir(os.getcwd())
    for item in current_dir_contents:
        print(f"   - {item}")
    
    if "src" not in current_dir_contents:
        print(f"   - ALERTA CRÍTICO: Pasta 'src' NÃO ENCONTRADA no diretório de trabalho atual.")
    else:
        # Tenta listar o conteúdo da pasta src
        src_path = os.path.join(os.getcwd(), "src")
        print(f"\n5. Conteúdo da pasta 'src' em '{src_path}':")
        src_contents = os.listdir(src_path)
        
        main_py_found = False
        init_py_found = False
        for item in src_contents:
            print(f"   - {item}")
            if item == "main.py":
                main_py_found = True
            if item == "__init__.py":
                init_py_found = True
        
        if not main_py_found:
            print(f"   - ALERTA CRÍTICO: 'main.py' NÃO ENCONTRADO em '{src_path}'.")
        if not init_py_found:
            print(f"   - ALERTA CRÍTICO: '__init__.py' NÃO ENCONTRADO em '{src_path}'. Isso PODE IMPEDIR que 'src' seja tratado como um pacote Python.")

except Exception as e:
    print(f"ERRO ao listar diretórios usando os.listdir: {e}. Isso é inesperado se o cwd é válido.")

# --- Verificação usando dbutils.fs.ls (se disponível) ---
# Esta parte é importante para entender se o 'Workspace' é mapeado para '/dbfs/' ou não para dbutils
if dbutils:
    try:
        print(f"\n6. Tentando listar '{project_root}' usando dbutils.fs.ls (se for um caminho DBFS):")
        # Nota: dbutils.fs.ls só funciona para caminhos /dbfs/ ou montagens.
        # Se '/Workspace' não for um ponto de montagem do DBFS, isso vai falhar,
        # o que é normal e esperado para Workspace Files.
        dbfs_files = dbutils.fs.ls(project_root) # Isso vai falhar se project_root for /Workspace/...
        print(f"   - Listagem dbutils.fs.ls bem-sucedida (o que seria inesperado para /Workspace/):")
        for f_info in dbfs_files:
            print(f"     - {f_info.name}")
    except Exception as e:
        print(f"   - ERRO esperado/normal ao usar dbutils.fs.ls com caminho do Workspace: {e}")
        print("     Isso confirma que '/Workspace' não é diretamente acessível via dbutils.fs.ls, mas está OK para importação via sys.path.")
else:
    print("dbutils não disponível, não foi possível verificar o sistema de arquivos com dbutils.fs.ls.")

print(f"\n--- FIM DO DIAGNÓSTICO DETALHADO ---")

# COMMAND ----------

# Bloco de importação e execução
try:
    from src.main import run_pipeline
    print("Importação de src.main bem-sucedida!")
    run_pipeline()
except ModuleNotFoundError as e:
    print(f"ERRO FINAL: ModuleNotFoundError persistente: {e}")
    print("\nResumo das causas mais prováveis para o 'ModuleNotFoundError' AGORA:")
    print("1. A pasta 'src' NÃO EXISTE (ou não está visível) DIRETAMENTE dentro do diretório retornado por os.getcwd().")
    print("2. O arquivo '__init__.py' está faltando dentro da pasta 'src'.")
    print("3. O arquivo 'main.py' está faltando dentro da pasta 'src'.")
    print("Por favor, examine o output do 'Diagnóstico de Path Detalhado' acima, especialmente os pontos 4 e 5, para ver o que está listado no diretório.") 