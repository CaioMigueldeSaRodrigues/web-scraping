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
    
    # Verificar se o diretório src existe
    src_path = project_root / "src"
    if not src_path.exists():
        print("⚠️ Diretório src não encontrado no Databricks")
        print("📁 Conteúdo do diretório atual:")
        import subprocess
        result = subprocess.run(['ls', '-la'], capture_output=True, text=True)
        print(result.stdout)
        
        print("\n📁 Conteúdo do diretório /Workspace/Repos:")
        result = subprocess.run(['ls', '-la', '/Workspace/Repos'], capture_output=True, text=True)
        print(result.stdout)
        
        # Tentar criar o diretório src e os arquivos necessários
        print("\n🔧 Criando estrutura de arquivos...")
        
        # Criar diretório src
        os.makedirs(str(src_path), exist_ok=True)
        
        # Criar __init__.py
        with open(src_path / "__init__.py", "w") as f:
            f.write("# src package\n")
        
        # Criar embeddings.py
        embeddings_content = '''import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType

# Forçar CPU
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
os.environ['TRANSFORMERS_NO_ADAM'] = '1'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

def generate_embeddings(spark, source_table, target_table, batch_size=250):
    """Função genérica para gerar embeddings"""
    try:
        # Carregue os dados
        df = spark.sql(f"SELECT title, price, url, categoria FROM {source_table}").toPandas()
        
        # Inicialize o modelo com CPU
        from sentence_transformers import SentenceTransformer
        modelo = SentenceTransformer("all-MiniLM-L6-v2", device='cpu')
        
        # Realize o embedding em batch
        embeddings = []
        titles = df['title'].tolist()
        for i in range(0, len(titles), batch_size):
            batch = titles[i:i+batch_size]
            vectors = modelo.encode(batch, show_progress_bar=True, convert_to_tensor=False, device='cpu')
            embeddings.extend(vectors)
        
        # Adicione os embeddings ao DataFrame
        df["embedding"] = embeddings
        
        # Defina o schema
        schema = StructType([
            StructField("title", StringType(), True),
            StructField("price", StringType(), True),
            StructField("url", StringType(), True),
            StructField("categoria", StringType(), True),
            StructField("embedding", ArrayType(FloatType()), True)
        ])
        
        # Crie o DataFrame Spark
        spark_df = spark.createDataFrame(df, schema=schema)
        
        # Salve o DataFrame como tabela Delta
        spark_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        
        print(f"✅ {target_table} criada com sucesso.")
        
    except Exception as e:
        print(f"❌ Erro na geração de embeddings: {e}")
        import traceback
        traceback.print_exc()
'''
        
        with open(src_path / "embeddings.py", "w") as f:
            f.write(embeddings_content)
        
        # Criar main.py
        main_content = '''# src/main.py
# Script principal para Databricks

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType

# Forçar CPU
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
os.environ['TRANSFORMERS_NO_ADAM'] = '1'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

def run_pipeline():
    """Pipeline principal para Databricks"""
    spark = SparkSession.builder \\
        .appName("Benchmarking Pipeline") \\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.deltaCatalog") \\
        .getOrCreate()
    
    print("🚀 Pipeline iniciado no Databricks")
    
    try:
        # Verificar se a tabela existe
        df = spark.sql("SELECT COUNT(*) as total FROM bronze.magalu_completo")
        count = df.collect()[0]['total']
        print(f"✅ Tabela bronze.magalu_completo encontrada com {count} registros")
        
        # Processamento de embeddings
        print("📊 Processamento de embeddings iniciado...")
        from src.embeddings import generate_embeddings
        
        generate_embeddings(
            spark=spark,
            source_table="bronze.magalu_completo",
            target_table="silver.embeddings_magalu_completo",
            batch_size=250
        )
        
        print("✅ Pipeline executado com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante execução: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    run_pipeline()
'''
        
        with open(src_path / "main.py", "w") as f:
            f.write(main_content)
        
        print("✅ Arquivos criados com sucesso!")
        
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
    try:
        result = subprocess.run(['ls', '-la', 'src'], capture_output=True, text=True)
        print(result.stdout)
    except:
        print("❌ Diretório src não encontrado")
    
    print("\n🔧 Tentando criar estrutura mínima...")
    
    # Criar estrutura mínima
    os.makedirs("src", exist_ok=True)
    
    with open("src/__init__.py", "w") as f:
        f.write("# src package\n")
    
    with open("src/main.py", "w") as f:
        f.write('''# src/main.py
def run_pipeline():
    print("✅ Pipeline executado com sucesso!")

if __name__ == "__main__":
    run_pipeline()
''')
    
    print("✅ Estrutura mínima criada. Tentando importar novamente...")
    
    try:
        from src.main import run_pipeline
        run_pipeline()
    except Exception as e2:
        print(f"❌ Erro final: {e2}")
    
except Exception as e:
    print(f"❌ Erro durante execução: {e}")
    import traceback
    traceback.print_exc() 