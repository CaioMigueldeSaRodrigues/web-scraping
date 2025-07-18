# Databricks notebook source

# COMMAND ----------
import sys
import os

# Tenta importar dbutils, ferramenta essencial no Databricks para gerenciar arquivos
try:
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(SparkSession.builder.getOrCreate())
except ImportError:
    dbutils = None # Caso não esteja em um ambiente Databricks completo, dbutils pode não estar disponível

# --- Bloco de Gerenciamento de Path (SOLUÇÃO DEFINITIVA FINAL) ---
# Define o caminho absoluto para a raiz do projeto no ambiente do Workspace Files.
# Este é o caminho correto que o cluster está enxergando seu projeto.
project_root = "/Workspace/Users/caiomiguel@bemol.com.br/web-scraping"

if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"--- Diagnóstico de Path Detalhado em Tempo de Execução ---")
print(f"1. project_root definido no notebook: {project_root}")
print(f"2. Conteúdo atual do sys.path (primeiros 5 itens): {sys.path[:5]}")
print(f"3. Diretório de trabalho atual (cwd): {os.getcwd()}") # Onde o notebook está sendo executado

# --- Verificações Cruciais no Sistema de Arquivos (DBFS) ---
if dbutils:
    try:
        print(f"\n4. Verificando o conteúdo de '{project_root}' no DBFS:")
        # dbutils.fs.ls retorna uma lista de FileInfo. Para printar, iteramos sobre ela.
        files_in_root = dbutils.fs.ls(project_root)
        print(f"   - Pasta '{project_root}' ENCONTRADA e acessível. Conteúdo:")
        for f in files_in_root:
            print(f"     - {f.name} (diretório: {f.isDir})") # f.name terá uma '/' no final se for diretório
        
        # --- Verificação da Pasta 'src' e seus arquivos ---
        src_path_in_dbfs = os.path.join(project_root, "src")
        print(f"\n5. Verificando a pasta 'src' em '{src_path_in_dbfs}' no DBFS:")
        
        src_found = False
        main_py_found = False
        init_py_found = False

        # Verifica se 'src' existe diretamente no project_root
        for f in files_in_root:
            if f.name == "src/" and f.isDir: # dbutils.fs.ls appends '/' for directories
                src_found = True
                break
        
        if src_found:
            print(f"   - Pasta 'src' ENCONTRADA dentro de '{project_root}'. Conteúdo:")
            files_in_src = dbutils.fs.ls(src_path_in_dbfs)
            for s_f in files_in_src:
                print(f"     - {s_f.name}")
                if s_f.name == "main.py":
                    main_py_found = True
                if s_f.name == "__init__.py":
                    init_py_found = True
            
            if not main_py_found:
                print(f"   - ALERTA CRÍTICO: 'main.py' NÃO ENCONTRADO em '{src_path_in_dbfs}'.")
            if not init_py_found:
                print(f"   - ALERTA: '__init__.py' NÃO ENCONTRADO em '{src_path_in_dbfs}'. Isso pode impedir que 'src' seja tratado como um pacote Python.")
        else:
            print(f"   - ERRO CRÍTICO: Pasta 'src' NÃO ENCONTRADA em '{project_root}'.")

    except Exception as e:
        print(f"4. ERRO ao acessar ou verificar paths no DBFS: {e}.")
        print(f"   Isso pode indicar que o caminho '{project_root}' está **incorreto no DBFS** ou que há um problema de permissões.")
else:
    print("4. dbutils não disponível. Não foi possível verificar o sistema de arquivos do DBFS diretamente.")

print(f"--- FIM DO DIAGNÓSTICO DETALHADO ---")


# COMMAND ----------

# Bloco de importação e execução
try:
    from src.main import run_pipeline
    print("Importação de src.main bem-sucedida!")
    run_pipeline()
except ModuleNotFoundError as e:
    print(f"ERRO FINAL: ModuleNotFoundError persistente: {e}")
    print("\nResumo das possíveis causas do erro 'ModuleNotFoundError' APÓS os ajustes:")
    print("1. O caminho do projeto ('project_root') no notebook *ainda não corresponde* ao local exato do projeto no sistema de arquivos do cluster (DBFS).")
    print("2. A estrutura de pastas ('src/', 'main.py', '__init__.py') *dentro* do projeto não está conforme o esperado.")
    print("3. O JOB ESTÁ EXECUTANDO UMA VERSÃO ANTIGA DO NOTEBOOK. Verifique a sincronização/atualização do notebook no Databricks Workspace.")
    print("   - Se você fez as alterações via `cat << EOF`, certifique-se de que o arquivo no seu Workspace foi realmente atualizado antes de enviar o job.")
    print("   - Se o job aponta para uma versão específica, verifique-a.")
    print("   - Tente abrir o notebook no Workspace e verificar manualmente o código antes de executar o job.") 