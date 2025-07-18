# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline de Benchmarking - Magalu vs Bemol
# MAGIC 
# MAGIC Este notebook executa o pipeline completo de análise de concorrência entre Magalu e Bemol.
# MAGIC 
# MAGIC ## Funcionalidades:
# MAGIC - Extração de dados das tabelas silver
# MAGIC - Cálculo de similaridade entre produtos
# MAGIC - Identificação de produtos exclusivos
# MAGIC - Análise de diferença de preços
# MAGIC - Geração de relatórios Excel e HTML
# MAGIC - Criação de TempView para consultas SQL
# MAGIC - Envio de relatórios por email

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração de Widgets

# COMMAND ----------

# DBTITLE 1,Configuração de Parâmetros
# Widgets para parametrização
dbutils.widgets.text("tabela_magalu", "silver.embeddings_magalu_completo", "Tabela Magalu")
dbutils.widgets.text("tabela_bemol", "silver.embeddings_bemol", "Tabela Bemol")
dbutils.widgets.text("caminho_excel", "benchmarking_produtos.xlsx", "Caminho Excel")
dbutils.widgets.text("caminho_html", "/dbfs/FileStore/relatorio_comparativo.html", "Caminho HTML")
dbutils.widgets.text("nome_tempview", "tempview_benchmarking_pares", "Nome TempView")

# Widgets para email
dbutils.widgets.dropdown("enviar_email", "false", ["true", "false"], "Enviar Email")
dbutils.widgets.text("destinatarios_email", "analytics@bemol.com.br", "Destinatários Email")
dbutils.widgets.text("assunto_email", "", "Assunto Email (opcional)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importação de Módulos

# COMMAND ----------

# DBTITLE 1,Importação de Bibliotecas
import sys
import os

# Adiciona o diretório src ao path
sys.path.append('/Workspace/Repos/web-scraping-main/src')

# Importa módulos do projeto
from src.main import (
    executar_pipeline_completo, 
    executar_pipeline_com_email,
    validar_parametros_pipeline
)
from src.logger_config import get_logger

# Configura logger
logger = get_logger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação de Parâmetros

# COMMAND ----------

# DBTITLE 1,Validação Inicial
# Obtém parâmetros dos widgets
tabela_magalu = dbutils.widgets.get("tabela_magalu")
tabela_bemol = dbutils.widgets.get("tabela_bemol")
caminho_excel = dbutils.widgets.get("caminho_excel")
caminho_html = dbutils.widgets.get("caminho_html")
nome_tempview = dbutils.widgets.get("nome_tempview")
enviar_email = dbutils.widgets.get("enviar_email").lower() == "true"
destinatarios_email = dbutils.widgets.get("destinatarios_email")
assunto_email = dbutils.widgets.get("assunto_email")

logger.info("Parâmetros configurados:")
logger.info(f"- Tabela Magalu: {tabela_magalu}")
logger.info(f"- Tabela Bemol: {tabela_bemol}")
logger.info(f"- Caminho Excel: {caminho_excel}")
logger.info(f"- Caminho HTML: {caminho_html}")
logger.info(f"- Nome TempView: {nome_tempview}")
logger.info(f"- Enviar Email: {enviar_email}")
logger.info(f"- Destinatários: {destinatarios_email}")

# Valida parâmetros
if not validar_parametros_pipeline(tabela_magalu, tabela_bemol):
    raise ValueError("Validação de parâmetros falhou. Verifique as tabelas de entrada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do Pipeline

# COMMAND ----------

# DBTITLE 1,Execução Principal
logger.info("Iniciando execução do pipeline de benchmarking...")

# Executa pipeline baseado na configuração de email
if enviar_email:
    # Converte string de destinatários para lista
    destinatarios_lista = [email.strip() for email in destinatarios_email.split(",")]
    
    # Executa pipeline com email
    resultados = executar_pipeline_com_email(
        tabela_magalu=tabela_magalu,
        tabela_bemol=tabela_bemol,
        caminho_excel=caminho_excel,
        caminho_html=caminho_html,
        destinatarios_email=destinatarios_lista,
        assunto_email=assunto_email if assunto_email else None
    )
else:
    # Executa pipeline sem email
    resultados = executar_pipeline_completo(
        tabela_magalu=tabela_magalu,
        tabela_bemol=tabela_bemol,
        caminho_excel=caminho_excel,
        caminho_html=caminho_html,
        enviar_email=False
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação de Resultados

# COMMAND ----------

# DBTITLE 1,Verificação de Status
if resultados["status"] == "sucesso":
    logger.info("✅ Pipeline executado com sucesso!")
    
    # Exibe estatísticas
    stats = resultados["estatisticas"]
    display(f"""
    ## 📊 Estatísticas do Relatório
    
    - **Total de Produtos**: {stats.get('total_produtos', 0)}
    - **Produtos Magalu**: {stats.get('produtos_magalu', 0)}
    - **Produtos Bemol**: {stats.get('produtos_bemol', 0)}
    - **Produtos Pareados**: {stats.get('produtos_pareados', 0)}
    - **Produtos Exclusivos**: {stats.get('produtos_exclusivos', 0)}
    
    ### Níveis de Similaridade:
    - **Muito Similar**: {stats.get('muito_similar', 0)}
    - **Moderadamente Similar**: {stats.get('moderadamente_similar', 0)}
    - **Pouco Similar**: {stats.get('pouco_similar', 0)}
    - **Exclusivo**: {stats.get('exclusivo', 0)}
    
    ### Arquivos Gerados:
    - **Excel**: {resultados["arquivo_excel"]}
    - **HTML**: {resultados["arquivo_html"]}
    - **TempView SQL**: {resultados["tempview_sql"]}
    
    ### Email:
    - **Enviado**: {'✅ Sim' if resultados.get('email_enviado', False) else '❌ Não'}
    """)
    
    # Exibe DataFrame final
    if resultados["dataframe_final"] is not None:
        display("## 📋 Dados Processados")
        display(resultados["dataframe_final"])
        
else:
    logger.error(f"❌ Pipeline falhou: {resultados.get('erro', 'Erro desconhecido')}")
    raise Exception(f"Pipeline falhou: {resultados.get('erro', 'Erro desconhecido')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas SQL de Exemplo

# COMMAND ----------

# DBTITLE 1,Exemplos de Consultas SQL
# Exemplo de consulta para produtos muito similares
query_muito_similar = f"""
SELECT title, marketplace, price, url, exclusividade, nivel_similaridade
FROM {nome_tempview}
WHERE nivel_similaridade = 'muito similar'
ORDER BY price DESC
LIMIT 10
"""

display("## 🔍 Produtos Muito Similares")
display(spark.sql(query_muito_similar))

# COMMAND ----------

# Exemplo de consulta para produtos exclusivos
query_exclusivos = f"""
SELECT title, marketplace, price, url, exclusividade
FROM {nome_tempview}
WHERE exclusividade = 'sim'
ORDER BY price DESC
LIMIT 10
"""

display("## 🏆 Produtos Exclusivos")
display(spark.sql(query_exclusivos))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acesso aos Arquivos Gerados

# COMMAND ----------

# DBTITLE 1,Links para Arquivos
# Exibe links para os arquivos gerados
display("## 📁 Arquivos Gerados")

# Link para Excel
if resultados["arquivo_excel"]:
    display(f"### 📊 Relatório Excel")
    display(f"Arquivo: `{resultados['arquivo_excel']}`")

# Link para HTML
if resultados["arquivo_html"]:
    display(f"### 🌐 Relatório HTML")
    display(f"Arquivo: `{resultados['arquivo_html']}`")
    display(f"URL: `/files/relatorio_comparativo.html`")

# TempView SQL
if resultados["tempview_sql"]:
    display(f"### 🗄️ TempView SQL")
    display(f"Nome: `{resultados['tempview_sql']}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalização

# COMMAND ----------

# DBTITLE 1,Resumo Final
logger.info("🎉 Pipeline de benchmarking concluído com sucesso!")
logger.info(f"📁 Arquivo Excel gerado: {resultados['arquivo_excel']}")
logger.info(f"🌐 Arquivo HTML gerado: {resultados['arquivo_html']}")
logger.info(f"🗄️ TempView criada: {resultados['tempview_sql']}")
logger.info(f"📊 Total de produtos processados: {resultados['total_produtos']}")

if enviar_email:
    email_status = "✅ Enviado" if resultados.get('email_enviado', False) else "❌ Falhou"
    logger.info(f"📧 Email: {email_status}")

display("## ✅ Pipeline Concluído!")
display(f"""
### 📋 Resumo da Execução:
- **Status**: {resultados["status"]}
- **Total de Produtos**: {resultados["total_produtos"]}
- **Arquivo Excel**: {resultados["arquivo_excel"]}
- **Arquivo HTML**: {resultados["arquivo_html"]}
- **TempView SQL**: {resultados["tempview_sql"]}
- **Email Enviado**: {'✅ Sim' if resultados.get('email_enviado', False) else '❌ Não'}

### 🔗 Próximos Passos:
1. Baixe o arquivo Excel gerado
2. Acesse o relatório HTML no navegador
3. Use a TempView para consultas SQL personalizadas
4. Analise os produtos exclusivos e similares
5. Monitore diferenças de preços entre marketplaces
""") 