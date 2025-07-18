# Databricks notebook source

# COMMAND ----------

# Pipeline completo para Databricks - embeddings, análises e relatórios
import os
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime

# Forçar CPU para evitar erros de CUDA
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
os.environ['TRANSFORMERS_NO_ADAM'] = '1'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

print("🚀 Pipeline completo iniciado no Databricks")

# COMMAND ----------

# Configurar SparkSession
spark = SparkSession.builder \
    .appName("Benchmarking Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.deltaCatalog") \
    .getOrCreate()

print("✅ SparkSession configurado")

# COMMAND ----------

# Função para gerar embeddings
def generate_embeddings_for_table(spark, source_table, target_table, batch_size=250):
    """Gera embeddings para uma tabela específica"""
    try:
        print(f"📊 Processando embeddings para {source_table}...")
        
        # Carregar dados
        df_data = spark.sql(f"SELECT title, price, url, categoria FROM {source_table}").toPandas()
        print(f"📊 Carregados {len(df_data)} registros de {source_table}")
        
        # Gerar embeddings
        from sentence_transformers import SentenceTransformer
        modelo = SentenceTransformer("all-MiniLM-L6-v2", device='cpu')
        print("✅ Modelo SentenceTransformer carregado")
        
        embeddings = []
        titles = df_data['title'].tolist()
        
        print(f"🔄 Processando {len(titles)} títulos em batches de {batch_size}...")
        
        for i in range(0, len(titles), batch_size):
            batch = titles[i:i+batch_size]
            vectors = modelo.encode(batch, show_progress_bar=True, convert_to_tensor=False, device='cpu')
            embeddings.extend(vectors)
            print(f"✅ Batch {i//batch_size + 1}/{(len(titles) + batch_size - 1)//batch_size} processado")
        
        # Adicionar embeddings
        df_data["embedding"] = embeddings
        print("✅ Embeddings adicionados ao DataFrame")
        
        # Schema
        schema = StructType([
            StructField("title", StringType(), True),
            StructField("price", StringType(), True),
            StructField("url", StringType(), True),
            StructField("categoria", StringType(), True),
            StructField("embedding", ArrayType(FloatType()), True)
        ])
        
        # Criar DataFrame Spark e salvar
        spark_df = spark.createDataFrame(df_data, schema=schema)
        spark_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        
        print(f"✅ {target_table} criada com sucesso!")
        return df_data
        
    except Exception as e:
        print(f"❌ Erro na geração de embeddings para {source_table}: {e}")
        import traceback
        traceback.print_exc()
        return None

# COMMAND ----------

# Função para análise de similaridade
def analyze_similarity(spark, table1, table2, output_table):
    """Analisa similaridade entre duas tabelas de embeddings"""
    try:
        print(f"🔍 Analisando similaridade entre {table1} e {table2}...")
        
        # Carregar embeddings
        df1 = spark.sql(f"SELECT title, price, url, categoria, embedding FROM {table1}").toPandas()
        df2 = spark.sql(f"SELECT title, price, url, categoria, embedding FROM {table2}").toPandas()
        
        print(f"📊 {len(df1)} produtos da {table1}")
        print(f"📊 {len(df2)} produtos da {table2}")
        
        # Converter embeddings para numpy arrays
        embeddings1 = np.array(df1['embedding'].tolist())
        embeddings2 = np.array(df2['embedding'].tolist())
        
        # Calcular similaridade
        similarity_matrix = cosine_similarity(embeddings1, embeddings2)
        
        # Encontrar produtos mais similares
        results = []
        for i, row in enumerate(similarity_matrix):
            # Top 5 produtos mais similares
            top_indices = np.argsort(row)[-5:][::-1]
            for j, idx in enumerate(top_indices):
                similarity_score = row[idx]
                if similarity_score > 0.7:  # Threshold de similaridade
                    results.append({
                        'produto_origem': df1.iloc[i]['title'],
                        'preco_origem': df1.iloc[i]['price'],
                        'categoria_origem': df1.iloc[i]['categoria'],
                        'produto_similar': df2.iloc[idx]['title'],
                        'preco_similar': df2.iloc[idx]['price'],
                        'categoria_similar': df2.iloc[idx]['categoria'],
                        'similaridade': float(similarity_score),
                        'data_analise': datetime.now().strftime('%Y-%m-%d')
                    })
        
        # Criar DataFrame de resultados
        if results:
            results_df = pd.DataFrame(results)
            
            # Schema para resultados
            schema = StructType([
                StructField("produto_origem", StringType(), True),
                StructField("preco_origem", StringType(), True),
                StructField("categoria_origem", StringType(), True),
                StructField("produto_similar", StringType(), True),
                StructField("preco_similar", StringType(), True),
                StructField("categoria_similar", StringType(), True),
                StructField("similaridade", FloatType(), True),
                StructField("data_analise", StringType(), True)
            ])
            
            # Salvar resultados
            spark_results = spark.createDataFrame(results_df, schema=schema)
            spark_results.write.format("delta").mode("overwrite").saveAsTable(output_table)
            
            print(f"✅ {output_table} criada com {len(results)} análises de similaridade!")
            return results_df
        else:
            print("⚠️ Nenhuma similaridade acima do threshold encontrada")
            return None
            
    except Exception as e:
        print(f"❌ Erro na análise de similaridade: {e}")
        import traceback
        traceback.print_exc()
        return None

# COMMAND ----------

# Função para enviar relatório via SendGrid
def send_report_via_sendgrid(analysis_results, report_date):
    """Envia relatório de análise via SendGrid"""
    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
        
        # Configurar SendGrid (usar dbutils.secrets no Databricks)
        try:
            api_key = dbutils.secrets.get(scope="sendgrid", key="api_key")
            from_email = dbutils.secrets.get(scope="sendgrid", key="from_email")
            to_email = dbutils.secrets.get(scope="sendgrid", key="to_email")
        except:
            print("⚠️ Usando configurações padrão para SendGrid")
            api_key = "YOUR_SENDGRID_API_KEY"  # Configurar no Databricks
            from_email = "relatorios@bemol.com.br"
            to_email = "analytics@bemol.com.br"
        
        # Criar conteúdo do e-mail
        subject = f"Relatório de Análise de Concorrência - {report_date}"
        
        # Criar HTML do relatório
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 10px; border-radius: 5px; }}
                .stats {{ margin: 20px 0; }}
                .product {{ border: 1px solid #ddd; margin: 10px 0; padding: 10px; border-radius: 5px; }}
                .similarity {{ color: #007bff; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>📊 Relatório de Análise de Concorrência</h2>
                <p><strong>Data:</strong> {report_date}</p>
            </div>
            
            <div class="stats">
                <h3>📈 Resumo da Análise</h3>
                <p><strong>Total de análises:</strong> {len(analysis_results) if analysis_results is not None else 0}</p>
                <p><strong>Produtos analisados:</strong> Magalu vs Tabela</p>
            </div>
        """
        
        if analysis_results is not None and len(analysis_results) > 0:
            html_content += "<h3>🔍 Produtos Similares Encontrados</h3>"
            
            for _, row in analysis_results.iterrows():
                similarity_percent = row['similaridade'] * 100
                html_content += f"""
                <div class="product">
                    <h4>📦 {row['produto_origem'][:50]}...</h4>
                    <p><strong>Preço:</strong> {row['preco_origem']} | <strong>Categoria:</strong> {row['categoria_origem']}</p>
                    <hr>
                    <h4>🔄 Similar: {row['produto_similar'][:50]}...</h4>
                    <p><strong>Preço:</strong> {row['preco_similar']} | <strong>Categoria:</strong> {row['categoria_similar']}</p>
                    <p class="similarity">📊 Similaridade: {similarity_percent:.1f}%</p>
                </div>
                """
        else:
            html_content += "<p>⚠️ Nenhum produto similar encontrado acima do threshold de 70%</p>"
        
        html_content += """
        </body>
        </html>
        """
        
        # Criar e-mail
        message = Mail(
            from_email=from_email,
            to_emails=to_email,
            subject=subject,
            html_content=html_content
        )
        
        # Enviar e-mail
        sg = SendGridAPIClient(api_key=api_key)
        response = sg.send(message)
        
        print(f"✅ Relatório enviado via SendGrid! Status: {response.status_code}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao enviar relatório via SendGrid: {e}")
        import traceback
        traceback.print_exc()
        return False

# COMMAND ----------

# Pipeline principal
try:
    print("🚀 Iniciando pipeline completo...")
    
    # 1. Gerar embeddings para Magalu
    df_magalu = generate_embeddings_for_table(
        spark=spark,
        source_table="bronze.magalu_completo",
        target_table="silver.embeddings_magalu_completo"
    )
    
    # 2. Gerar embeddings para Tabela
    df_tabela = generate_embeddings_for_table(
        spark=spark,
        source_table="bronze.tabela_completo",
        target_table="silver.embeddings_tabela_completo"
    )
    
    # 3. Análise de similaridade
    if df_magalu is not None and df_tabela is not None:
        analysis_results = analyze_similarity(
            spark=spark,
            table1="silver.embeddings_magalu_completo",
            table2="silver.embeddings_tabela_completo",
            output_table="gold.analise_similaridade_magalu_tabela"
        )
        
        # 4. Enviar relatório via SendGrid
        report_date = datetime.now().strftime('%Y-%m-%d')
        send_report_via_sendgrid(analysis_results, report_date)
    
    print("✅ Pipeline completo executado com sucesso!")
    
except Exception as e:
    print(f"❌ Erro durante execução: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# Parar a sessão Spark
spark.stop()
print("✅ SparkSession finalizada") 