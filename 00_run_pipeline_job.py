# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Pipeline de Benchmarking - Bemol vs Magalu
# MAGIC 
# MAGIC Este notebook executa o pipeline completo de benchmarking entre Bemol e Magazine Luiza.
# MAGIC 
# MAGIC ## 📋 Funcionalidades:
# MAGIC - ✅ Análise de similaridade de produtos via embeddings
# MAGIC - ✅ Remoção de duplicados com 100% similaridade
# MAGIC - ✅ Geração de relatórios Excel e HTML
# MAGIC - ✅ Envio automático de email (opcional)
# MAGIC - ✅ Criação de TempView para consultas SQL
# MAGIC 
# MAGIC ## ⚙️ Configuração via Widgets:
# MAGIC - `tabela_magalu`: Tabela com embeddings do Magalu
# MAGIC - `tabela_bemol`: Tabela com embeddings da Bemol
# MAGIC - `caminho_excel`: Caminho do arquivo Excel
# MAGIC - `caminho_html`: Caminho do arquivo HTML
# MAGIC - `nome_tempview`: Nome da TempView SQL
# MAGIC - `enviar_email`: Se deve enviar email (true/false)
# MAGIC - `destinatarios_email`: Lista de emails destinatários
# MAGIC - `assunto_email`: Assunto do email

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuração de Widgets

# COMMAND ----------

# Configuração de widgets para parameterização
dbutils.widgets.text("tabela_magalu", "silver.embeddings_magalu_completo", "Tabela Magalu")
dbutils.widgets.text("tabela_bemol", "bol.feed_varejo_vtex", "Tabela Bemol")
dbutils.widgets.text("caminho_excel", "benchmarking_completo.xlsx", "Caminho Excel")
dbutils.widgets.text("caminho_html", "/dbfs/FileStore/relatorio_comparativo.html", "Caminho HTML")
dbutils.widgets.text("nome_tempview", "tempview_benchmarking_pares", "Nome TempView")
dbutils.widgets.dropdown("enviar_email", "false", ["true", "false"], "Enviar Email")
dbutils.widgets.text("destinatarios_email", "renatobolf@bemol.com.br", "Destinatários Email")
dbutils.widgets.text("assunto_email", "Scraping - Benchmarking de produtos", "Assunto Email")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Debug: Verificar Tabelas Disponíveis

# COMMAND ----------

# Célula de debug para verificar tabelas disponíveis
try:
    import sys
    import os
    
    # Adiciona o diretório src ao path
    sys.path.append('/Workspace/Repos/caio.miguel@bemol.com.br/web-scraping-main/src')
    
    print("🔍 Verificando tabelas disponíveis no catálogo...")
    
    # Tenta importar a função diretamente
    try:
        from src.main import listar_tabelas_disponiveis
        print("✅ Função listar_tabelas_disponiveis importada com sucesso")
    except ImportError as e:
        print(f"❌ Erro ao importar listar_tabelas_disponiveis: {e}")
        print("🔧 Tentando import alternativo...")
        
        # Tenta importar o módulo completo
        try:
            import src.main as main_module
            listar_tabelas_disponiveis = main_module.listar_tabelas_disponiveis
            print("✅ Função encontrada via import alternativo")
        except Exception as e2:
            print(f"❌ Erro no import alternativo: {e2}")
            raise
    
    # Executa a função
    tabelas_info = listar_tabelas_disponiveis()
    
    print("\n📊 Tabelas encontradas:")
    for nome_tabela, info in tabelas_info.items():
        if "error" in info:
            print(f"❌ {nome_tabela}: ERRO - {info['error']}")
        else:
            print(f"✅ {nome_tabela}: {info['count']} registros, {len(info['columns'])} colunas")
            print(f"   Colunas: {info['columns']}")
    
    # Verifica tabelas específicas
    tabela_magalu = dbutils.widgets.get("tabela_magalu")
    tabela_bemol = dbutils.widgets.get("tabela_bemol")
    
    print(f"\n🎯 Verificando tabelas do pipeline:")
    print(f"Tabela Magalu: {tabela_magalu}")
    print(f"Tabela Bemol: {tabela_bemol}")
    
    if tabela_magalu in tabelas_info:
        print(f"✅ Tabela Magalu encontrada")
    else:
        print(f"❌ Tabela Magalu NÃO encontrada")
        
    if tabela_bemol in tabelas_info:
        print(f"✅ Tabela Bemol encontrada")
    else:
        print(f"❌ Tabela Bemol NÃO encontrada")
        
except Exception as e:
    print(f"❌ Erro ao verificar tabelas: {e}")
    import traceback
    traceback.print_exc()
    
    # Fallback: lista tabelas de forma básica
    print("\n🔄 Tentando listagem básica de tabelas...")
    try:
        tabelas_basicas = spark.catalog.listTables()
        print("📋 Tabelas disponíveis no catálogo:")
        for tabela in tabelas_basicas:
            print(f"  - {tabela.name} ({tabela.database})")
    except Exception as e2:
        print(f"❌ Erro na listagem básica: {e2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execução do Pipeline

# COMMAND ----------

# Importa módulos necessários
import sys
import os

# Adiciona o diretório src ao path
sys.path.append('/Workspace/Repos/caio.miguel@bemol.com.br/web-scraping-main/src')

# Importa módulos do projeto usando imports diretos
try:
    from src.main import (
        executar_pipeline_completo_com_email,
        listar_tabelas_disponiveis
    )
    from src.logger_config import get_logger
    print("✅ Imports diretos bem-sucedidos")
except ImportError as e:
    print(f"❌ Erro no import direto: {e}")
    print("🔧 Tentando import alternativo...")
    
    try:
        import src.main as main_module
        import src.logger_config as logger_module
        
        executar_pipeline_completo_com_email = main_module.executar_pipeline_completo_com_email
        listar_tabelas_disponiveis = main_module.listar_tabelas_disponiveis
        get_logger = logger_module.get_logger
        
        print("✅ Imports alternativos bem-sucedidos")
    except Exception as e2:
        print(f"❌ Erro no import alternativo: {e2}")
        raise

# Configura logger
logger = get_logger(__name__)

# COMMAND ----------

# Obtém parâmetros dos widgets
tabela_magalu = dbutils.widgets.get("tabela_magalu")
tabela_bemol = dbutils.widgets.get("tabela_bemol")
caminho_excel = dbutils.widgets.get("caminho_excel")
caminho_html = dbutils.widgets.get("caminho_html")
nome_tempview = dbutils.widgets.get("nome_tempview")
enviar_email = dbutils.widgets.get("enviar_email") == "true"
destinatarios_email = dbutils.widgets.get("destinatarios_email")
assunto_email = dbutils.widgets.get("assunto_email")

# Converte string de emails para lista
if destinatarios_email:
    destinatarios_lista = [email.strip() for email in destinatarios_email.split(",")]
else:
    destinatarios_lista = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Executando Pipeline

# COMMAND ----------

try:
    print("🚀 Iniciando pipeline de benchmarking...")
    print(f"📋 Parâmetros:")
    print(f"  - Tabela Magalu: {tabela_magalu}")
    print(f"  - Tabela Bemol: {tabela_bemol}")
    print(f"  - Caminho Excel: {caminho_excel}")
    print(f"  - Caminho HTML: {caminho_html}")
    print(f"  - Nome TempView: {nome_tempview}")
    print(f"  - Enviar Email: {enviar_email}")
    print(f"  - Destinatários: {destinatarios_lista}")
    print(f"  - Assunto: {assunto_email}")
    
    # Executa pipeline
    resultados = executar_pipeline_completo_com_email(
        tabela_magalu=tabela_magalu,
        tabela_bemol=tabela_bemol,
        caminho_excel=caminho_excel,
        caminho_html=caminho_html,
        nome_tempview=nome_tempview,
        enviar_email=enviar_email,
        destinatarios_email=destinatarios_lista,
        assunto_email=assunto_email
    )
    
    # Verifica resultados
    if resultados["status"] == "sucesso":
        print("\n✅ Pipeline executado com sucesso!")
        print(f"📊 Estatísticas:")
        stats = resultados["estatisticas"]
        print(f"  - Total de produtos: {stats.get('total_produtos', 0)}")
        print(f"  - Produtos pareados: {stats.get('produtos_pareados', 0)}")
        print(f"  - Produtos exclusivos: {stats.get('produtos_exclusivos', 0)}")
        print(f"  - Produtos Magalu: {stats.get('produtos_magalu', 0)}")
        print(f"  - Produtos Bemol: {stats.get('produtos_bemol', 0)}")
        
        print(f"\n📁 Arquivos gerados:")
        print(f"  - Excel: {resultados['caminho_excel']}")
        print(f"  - HTML: {resultados['caminho_html']}")
        print(f"  - TempView: {resultados['nome_tempview']}")
        
        if enviar_email:
            print(f"\n📧 Email:")
            print(f"  - Enviado: {resultados.get('email_enviado', False)}")
            print(f"  - Destinatários: {destinatarios_lista}")
        
        # Exibe DataFrame final
        df_final = resultados["df_final"]
        print(f"\n📋 Resumo do DataFrame final:")
        print(f"  - Shape: {df_final.shape}")
        print(f"  - Colunas: {list(df_final.columns)}")
        
        # Exibe primeiras linhas
        display(df_final.head(10))
        
    else:
        print(f"\n❌ Erro no pipeline: {resultados.get('erro', 'Erro desconhecido')}")
        
except Exception as e:
    print(f"❌ Erro crítico no pipeline: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Consultas SQL de Exemplo

# COMMAND ----------

# Exemplo de consultas SQL para a TempView criada
if 'resultados' in locals() and resultados.get("status") == "sucesso":
    nome_tempview = resultados["nome_tempview"]
    
    print(f"🔍 Exemplos de consultas SQL para '{nome_tempview}':")
    
    # Consulta 1: Produtos pareados ordenados por similaridade
    query1 = f"""
    SELECT title, marketplace, price, exclusividade, similaridade
    FROM {nome_tempview}
    WHERE exclusividade = 'não'
    ORDER BY similaridade DESC
    LIMIT 10
    """
    
    print(f"\n1. Top 10 produtos mais similares:")
    print(query1)
    
    # Consulta 2: Produtos exclusivos
    query2 = f"""
    SELECT title, marketplace, price, url
    FROM {nome_tempview}
    WHERE exclusividade = 'sim'
    ORDER BY marketplace, price DESC
    """
    
    print(f"\n2. Produtos exclusivos:")
    print(query2)
    
    # Consulta 3: Estatísticas por marketplace
    query3 = f"""
    SELECT 
        marketplace,
        COUNT(*) as total_produtos,
        COUNT(CASE WHEN exclusividade = 'sim' THEN 1 END) as exclusivos,
        COUNT(CASE WHEN exclusividade = 'não' THEN 1 END) as pareados,
        AVG(price) as preco_medio
    FROM {nome_tempview}
    GROUP BY marketplace
    """
    
    print(f"\n3. Estatísticas por marketplace:")
    print(query3)
    
    # Executa consulta de exemplo
    try:
        print(f"\n📊 Executando consulta de exemplo...")
        df_exemplo = spark.sql(query1)
        display(df_exemplo)
    except Exception as e:
        print(f"❌ Erro ao executar consulta: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Links para Arquivos Gerados

# COMMAND ----------

# Exibe links para os arquivos gerados
if 'resultados' in locals() and resultados.get("status") == "sucesso":
    print("📁 Acesso aos Arquivos Gerados:")
    
    # Link para Excel
    excel_path = resultados["caminho_excel"]
    print(f"📊 Relatório Excel: {excel_path}")
    
    # Link para HTML
    html_path = resultados["caminho_html"]
    print(f"🌐 Relatório HTML: {html_path}")
    
    # Link para TempView
    tempview_name = resultados["nome_tempview"]
    print(f"🔍 TempView SQL: {tempview_name}")
    
    # Comandos para download (se necessário)
    print(f"\n💾 Comandos para download:")
    print(f"# Download do Excel")
    print(f"dbutils.fs.cp('{excel_path}', '/tmp/benchmarking.xlsx')")
    print(f"dbutils.fs.ls('/tmp/benchmarking.xlsx')")
    
    print(f"\n# Download do HTML")
    print(f"dbutils.fs.cp('{html_path}', '/tmp/relatorio.html')")
    print(f"dbutils.fs.ls('/tmp/relatorio.html')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Pipeline Concluído
# MAGIC 
# MAGIC O pipeline de benchmarking foi executado com sucesso!
# MAGIC 
# MAGIC ### 📊 Próximos Passos:
# MAGIC 1. **Analisar relatórios** gerados
# MAGIC 2. **Consultar TempView** para análises adicionais
# MAGIC 3. **Configurar job** para execução automática
# MAGIC 4. **Implementar dashboard** Power BI
# MAGIC 
# MAGIC ### 🔧 Configuração de Job:
# MAGIC - **Notebook**: Este notebook
# MAGIC - **Cluster**: Runtime 11.3+ com 2+ workers
# MAGIC - **Agendamento**: Diário às 06:00 AM
# MAGIC - **Parâmetros**: Configurar widgets conforme necessário 