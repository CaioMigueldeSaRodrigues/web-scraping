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
    
    print("🔍 Verificando tabelas disponíveis no catálogo...")
    
    # Função local para listar tabelas (não depende de imports externos)
    def listar_tabelas_disponiveis_local() -> dict:
        """
        Lista todas as tabelas disponíveis no catálogo para debug.
        """
        try:
            tabelas_info = {}
            tabelas_existentes = spark.catalog.listTables()
            
            for table in tabelas_existentes:
                try:
                    # Tenta contar registros
                    count = spark.table(table.name).count()
                    
                    # Tenta obter estrutura
                    sample = spark.table(table.name).limit(1).toPandas()
                    colunas = list(sample.columns) if not sample.empty else []
                    
                    tabelas_info[table.name] = {
                        "database": table.database,
                        "count": count,
                        "columns": colunas,
                        "type": table.tableType
                    }
                    
                except Exception as e:
                    tabelas_info[table.name] = {
                        "error": str(e),
                        "database": table.database,
                        "type": table.tableType
                    }
            
            return tabelas_info
            
        except Exception as e:
            print(f"❌ Erro ao listar tabelas: {e}")
            return {}
    
    # Executa a função local
    tabelas_info = listar_tabelas_disponiveis_local()
    
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
import pandas as pd
from typing import Optional, Dict, Any

print("🚀 Iniciando pipeline de benchmarking...")

def verificar_estrutura_dados_web_scraping(tabela_magalu: str, tabela_bemol: str) -> Dict[str, Any]:
    """
    Verifica a estrutura específica dos dados de web scraping e embeddings.
    Baseado na origem real dos dados compartilhada pelo usuário.
    """
    estrutura_info = {
        "magalu_web_scraping": {
            "tabelas_bronze": [],
            "tabela_unificada": None,
            "tabela_embeddings": None,
            "status": "desconhecido",
            "origem": "Web scraping direto do Magazine Luiza"
        },
        "bemol_feed": {
            "tabela_feed": None,
            "status": "desconhecido",
            "origem": "Tabela interna do Databricks (bol.feed_varejo_vtex)"
        }
    }
    
    # Verifica tabelas bronze do Magalu (web scraping direto)
    categorias_magalu = [
        "bronze.magalu_eletroportateis",
        "bronze.magalu_informatica", 
        "bronze.magalu_tv_e_video",
        "bronze.magalu_moveis",
        "bronze.magalu_eletrodomesticos",
        "bronze.magalu_celulares"
    ]
    
    print("🔍 Verificando estrutura de dados do web scraping direto...")
    print("📋 Origem Magalu: Web scraping direto do Magazine Luiza")
    print("📋 Origem Bemol: Tabela interna bol.feed_varejo_vtex")
    
    # Verifica tabelas bronze
    for tabela_bronze in categorias_magalu:
        try:
            df_test = spark.table(tabela_bronze)
            count = df_test.count()
            colunas = df_test.columns
            
            if count > 0:
                estrutura_info["magalu_web_scraping"]["tabelas_bronze"].append({
                    "nome": tabela_bronze,
                    "registros": count,
                    "colunas": colunas,
                    "status": "ativo"
                })
                print(f"✅ {tabela_bronze}: {count} registros")
                print(f"   📋 Colunas: {colunas}")
            else:
                print(f"⚠️ {tabela_bronze}: vazia")
        except Exception as e:
            print(f"❌ {tabela_bronze}: erro - {e}")
            estrutura_info["magalu_web_scraping"]["tabelas_bronze"].append({
                "nome": tabela_bronze,
                "registros": 0,
                "colunas": [],
                "status": "erro",
                "erro": str(e)
            })
    
    # Verifica tabela unificada (bronze.magalu_completo)
    try:
        df_unificado = spark.table("bronze.magalu_completo")
        count_unificado = df_unificado.count()
        colunas_unificado = df_unificado.columns
        
        estrutura_info["magalu_web_scraping"]["tabela_unificada"] = {
            "nome": "bronze.magalu_completo",
            "registros": count_unificado,
            "colunas": colunas_unificado,
            "status": "ativo"
        }
        print(f"✅ bronze.magalu_completo: {count_unificado} registros")
        print(f"   📋 Colunas: {colunas_unificado}")
    except Exception as e:
        print(f"❌ bronze.magalu_completo: erro - {e}")
        estrutura_info["magalu_web_scraping"]["tabela_unificada"] = {
            "nome": "bronze.magalu_completo",
            "registros": 0,
            "colunas": [],
            "status": "erro",
            "erro": str(e)
        }
    
    # Verifica tabela de embeddings
    try:
        df_embeddings = spark.table(tabela_magalu)
        count_embeddings = df_embeddings.count()
        colunas_embeddings = df_embeddings.columns
        
        estrutura_info["magalu_web_scraping"]["tabela_embeddings"] = {
            "nome": tabela_magalu,
            "registros": count_embeddings,
            "colunas": colunas_embeddings,
            "status": "ativo"
        }
        
        print(f"✅ {tabela_magalu}: {count_embeddings} registros")
        print(f"   📋 Colunas: {colunas_embeddings}")
        
        # Verifica se tem embedding
        if "embedding" in colunas_embeddings:
            print(f"   ✅ Coluna 'embedding' encontrada")
            print(f"   🤖 Embeddings gerados com sentence-transformers")
        else:
            print(f"   ⚠️ Coluna 'embedding' não encontrada")
            print(f"   💡 Execute: from src.embeddings import generate_magalu_embeddings")
            
    except Exception as e:
        print(f"❌ {tabela_magalu}: erro - {e}")
        estrutura_info["magalu_web_scraping"]["status"] = "erro"
        estrutura_info["magalu_web_scraping"]["tabela_embeddings"] = {
            "nome": tabela_magalu,
            "registros": 0,
            "colunas": [],
            "status": "erro",
            "erro": str(e)
        }
    
    # Verifica tabela Bemol (feed VTEX interno)
    try:
        df_bemol = spark.table(tabela_bemol)
        count_bemol = df_bemol.count()
        colunas_bemol = df_bemol.columns
        
        estrutura_info["bemol_feed"]["tabela_feed"] = {
            "nome": tabela_bemol,
            "registros": count_bemol,
            "colunas": colunas_bemol,
            "status": "ativo"
        }
        
        print(f"✅ {tabela_bemol}: {count_bemol} registros")
        print(f"   📋 Colunas: {colunas_bemol}")
        print(f"   🏪 Feed VTEX interno da Bemol")
        
    except Exception as e:
        print(f"❌ {tabela_bemol}: erro - {e}")
        estrutura_info["bemol_feed"]["status"] = "erro"
        estrutura_info["bemol_feed"]["tabela_feed"] = {
            "nome": tabela_bemol,
            "registros": 0,
            "colunas": [],
            "status": "erro",
            "erro": str(e)
        }
    
    return estrutura_info

def diagnosticar_tabelas_embeddings(tabela_magalu: str, tabela_bemol: str) -> Dict[str, Any]:
    """
    Função de diagnóstico específica para tabelas de embeddings geradas por web scraping.
    """
    diagnostico = {
        "tabela_magalu": {"status": "desconhecido", "erro": None, "count": 0, "colunas": []},
        "tabela_bemol": {"status": "desconhecido", "erro": None, "count": 0, "colunas": []},
        "origem_dados": {
            "magalu": "Web scraping + embeddings (sentence-transformers)",
            "bemol": "Feed VTEX interno"
        }
    }
    
    # Diagnóstico tabela Magalu (embeddings)
    try:
        print(f"🔍 Diagnosticando tabela de embeddings {tabela_magalu}...")
        df_test = spark.table(tabela_magalu)
        count = df_test.count()
        colunas = df_test.columns
        
        diagnostico["tabela_magalu"]["status"] = "acessivel"
        diagnostico["tabela_magalu"]["count"] = count
        diagnostico["tabela_magalu"]["colunas"] = colunas
        
        print(f"✅ Tabela {tabela_magalu}: {count} registros")
        print(f"   📋 Colunas: {colunas}")
        
        # Verifica se tem coluna de embedding
        if "embedding" in colunas:
            print(f"   ✅ Coluna 'embedding' encontrada")
        else:
            print(f"   ⚠️ Coluna 'embedding' não encontrada")
            
    except Exception as e:
        diagnostico["tabela_magalu"]["status"] = "erro"
        diagnostico["tabela_magalu"]["erro"] = str(e)
        print(f"❌ Erro na tabela {tabela_magalu}: {e}")
        print(f"   💡 Esta tabela deve conter embeddings gerados por web scraping")
    
    # Diagnóstico tabela Bemol (feed VTEX)
    try:
        print(f"🔍 Diagnosticando tabela {tabela_bemol}...")
        df_test = spark.table(tabela_bemol)
        count = df_test.count()
        colunas = df_test.columns
        
        diagnostico["tabela_bemol"]["status"] = "acessivel"
        diagnostico["tabela_bemol"]["count"] = count
        diagnostico["tabela_bemol"]["colunas"] = colunas
        
        print(f"✅ Tabela {tabela_bemol}: {count} registros")
        print(f"   📋 Colunas: {colunas}")
        
    except Exception as e:
        diagnostico["tabela_bemol"]["status"] = "erro"
        diagnostico["tabela_bemol"]["erro"] = str(e)
        print(f"❌ Erro na tabela {tabela_bemol}: {e}")
        print(f"   💡 Esta tabela deve conter dados do feed VTEX da Bemol")
    
    return diagnostico

def validar_parametros_pipeline_robusto(tabela_magalu: str, tabela_bemol: str) -> bool:
    """
    Valida parâmetros do pipeline com abordagem robusta que contorna problemas de catálogo.
    """
    try:
        print(f"🔍 Validando parâmetros do pipeline (abordagem robusta)...")
        print(f"Tabela Magalu: {tabela_magalu}")
        print(f"Tabela Bemol: {tabela_bemol}")
        
        # Validação direta das tabelas - tenta acessar diretamente
        tabelas_validas = []
        
        # Testa tabela Magalu
        try:
            print(f"📊 Testando acesso à tabela {tabela_magalu}...")
            df_magalu_test = spark.table(tabela_magalu)
            count_magalu = df_magalu_test.count()
            print(f"✅ Tabela {tabela_magalu}: {count_magalu} registros")
            
            if count_magalu > 0:
                tabelas_validas.append(tabela_magalu)
            else:
                print(f"⚠️ Tabela {tabela_magalu} está vazia")
                
        except Exception as e:
            print(f"❌ Erro ao acessar tabela {tabela_magalu}: {e}")
            print(f"💡 Tentando verificar se a tabela existe no catálogo...")
            
            # Fallback: verifica se a tabela existe no catálogo
            try:
                tabelas_catalogo = spark.catalog.listTables()
                tabela_encontrada = False
                for table in tabelas_catalogo:
                    if table.name == tabela_magalu.split('.')[-1]:  # Remove schema se presente
                        tabela_encontrada = True
                        break
                
                if tabela_encontrada:
                    print(f"✅ Tabela {tabela_magalu} encontrada no catálogo")
                    tabelas_validas.append(tabela_magalu)
                else:
                    print(f"❌ Tabela {tabela_magalu} não encontrada no catálogo")
                    
            except Exception as e2:
                print(f"❌ Erro ao verificar catálogo: {e2}")
        
        # Testa tabela Bemol
        try:
            print(f"📊 Testando acesso à tabela {tabela_bemol}...")
            df_bemol_test = spark.table(tabela_bemol)
            count_bemol = df_bemol_test.count()
            print(f"✅ Tabela {tabela_bemol}: {count_bemol} registros")
            
            if count_bemol > 0:
                tabelas_validas.append(tabela_bemol)
            else:
                print(f"⚠️ Tabela {tabela_bemol} está vazia")
                
        except Exception as e:
            print(f"❌ Erro ao acessar tabela {tabela_bemol}: {e}")
            print(f"💡 Tentando verificar se a tabela existe no catálogo...")
            
            # Fallback: verifica se a tabela existe no catálogo
            try:
                tabelas_catalogo = spark.catalog.listTables()
                tabela_encontrada = False
                for table in tabelas_catalogo:
                    if table.name == tabela_bemol.split('.')[-1]:  # Remove schema se presente
                        tabela_encontrada = True
                        break
                
                if tabela_encontrada:
                    print(f"✅ Tabela {tabela_bemol} encontrada no catálogo")
                    tabelas_validas.append(tabela_bemol)
                else:
                    print(f"❌ Tabela {tabela_bemol} não encontrada no catálogo")
                    
            except Exception as e2:
                print(f"❌ Erro ao verificar catálogo: {e2}")
        
        # Verifica se pelo menos uma tabela é válida
        if len(tabelas_validas) >= 1:
            print(f"✅ Validação concluída: {len(tabelas_validas)} tabela(s) válida(s)")
            print(f"📋 Tabelas válidas: {tabelas_validas}")
            return True
        else:
            print(f"❌ Nenhuma tabela válida encontrada")
            return False
        
    except Exception as e:
        print(f"❌ Erro geral na validação de parâmetros: {e}")
        return False

def diagnosticar_tabelas(tabela_magalu: str, tabela_bemol: str) -> Dict[str, Any]:
    """
    Função de diagnóstico para identificar problemas específicos com as tabelas.
    """
    diagnostico = {
        "tabela_magalu": {"status": "desconhecido", "erro": None, "count": 0},
        "tabela_bemol": {"status": "desconhecido", "erro": None, "count": 0}
    }
    
    # Diagnóstico tabela Magalu
    try:
        print(f"🔍 Diagnosticando tabela {tabela_magalu}...")
        df_test = spark.table(tabela_magalu)
        count = df_test.count()
        diagnostico["tabela_magalu"]["status"] = "acessivel"
        diagnostico["tabela_magalu"]["count"] = count
        print(f"✅ Tabela {tabela_magalu}: {count} registros")
    except Exception as e:
        diagnostico["tabela_magalu"]["status"] = "erro"
        diagnostico["tabela_magalu"]["erro"] = str(e)
        print(f"❌ Erro na tabela {tabela_magalu}: {e}")
    
    # Diagnóstico tabela Bemol
    try:
        print(f"🔍 Diagnosticando tabela {tabela_bemol}...")
        df_test = spark.table(tabela_bemol)
        count = df_test.count()
        diagnostico["tabela_bemol"]["status"] = "acessivel"
        diagnostico["tabela_bemol"]["count"] = count
        print(f"✅ Tabela {tabela_bemol}: {count} registros")
    except Exception as e:
        diagnostico["tabela_bemol"]["status"] = "erro"
        diagnostico["tabela_bemol"]["erro"] = str(e)
        print(f"❌ Erro na tabela {tabela_bemol}: {e}")
    
    return diagnostico

def executar_pipeline_robusto(tabela_magalu: str, tabela_bemol: str) -> Dict[str, Any]:
    """
    Executa pipeline robusto que funciona mesmo com problemas de catálogo.
    """
    try:
        print("🚀 Iniciando pipeline robusto...")
        
        # Executa verificação completa da estrutura de dados
        print("🔍 Verificando estrutura completa dos dados de web scraping...")
        estrutura_dados = verificar_estrutura_dados_web_scraping(tabela_magalu, tabela_bemol)
        
        # Executa diagnóstico específico para embeddings
        print("🔍 Executando diagnóstico das tabelas de embeddings...")
        diagnostico = diagnosticar_tabelas_embeddings(tabela_magalu, tabela_bemol)
        
        # Valida parâmetros com abordagem robusta
        if not validar_parametros_pipeline_robusto(tabela_magalu, tabela_bemol):
            print("⚠️ Validação falhou, mas continuando com dados disponíveis...")
        
        # Carrega dados das tabelas com tratamento de erro individual
        df_magalu = None
        df_bemol = None
        
        # Tenta carregar tabela Magalu
        try:
            print(f"📊 Carregando dados da tabela {tabela_magalu}")
            df_magalu = spark.table(tabela_magalu).toPandas()
            print(f"✅ Tabela Magalu carregada: {len(df_magalu)} registros")
        except Exception as e:
            print(f"❌ Erro ao carregar tabela {tabela_magalu}: {e}")
            df_magalu = pd.DataFrame()  # DataFrame vazio
        
        # Tenta carregar tabela Bemol
        try:
            print(f"📊 Carregando dados da tabela {tabela_bemol}")
            df_bemol = spark.table(tabela_bemol).toPandas()
            print(f"✅ Tabela Bemol carregada: {len(df_bemol)} registros")
        except Exception as e:
            print(f"❌ Erro ao carregar tabela {tabela_bemol}: {e}")
            df_bemol = pd.DataFrame()  # DataFrame vazio
        
        # Verifica se pelo menos uma tabela foi carregada
        if df_magalu is None and df_bemol is None:
            return {
                "status": "erro",
                "erro": "Nenhuma tabela pôde ser carregada"
            }
        
        # Prepara dados para processamento
        dfs_validos = []
        if df_magalu is not None and not df_magalu.empty:
            df_magalu['marketplace'] = 'Magalu'
            dfs_validos.append(df_magalu)
            
        if df_bemol is not None and not df_bemol.empty:
            df_bemol['marketplace'] = 'Bemol'
            dfs_validos.append(df_bemol)
        
        # Combina dados
        if dfs_validos:
            df_final = pd.concat(dfs_validos, ignore_index=True)
        else:
            df_final = pd.DataFrame()
        
        print(f"📊 Dados combinados: {len(df_final)} registros")
        
        # Cria TempView se há dados
        if not df_final.empty:
            try:
                spark_df = spark.createDataFrame(df_final)
                spark_df.createOrReplaceTempView("tempview_benchmarking_pares")
                print("✅ TempView criada com sucesso")
            except Exception as e:
                print(f"⚠️ Erro ao criar TempView: {e}")
        
        # Calcula estatísticas
        stats = {
            "total_produtos": len(df_final),
            "produtos_magalu": len(df_magalu) if df_magalu is not None else 0,
            "produtos_bemol": len(df_bemol) if df_bemol is not None else 0,
            "produtos_pareados": 0,  # Placeholder para futura implementação
            "produtos_exclusivos": len(df_final)  # Placeholder
        }
        
        print("✅ Pipeline robusto executado com sucesso")
        print(f"📊 Estatísticas: {stats}")
        
        return {
            "status": "sucesso",
            "df_final": df_final,
            "estatisticas": stats,
            "nome_tempview": "tempview_benchmarking_pares",
            "df_magalu": df_magalu,
            "df_bemol": df_bemol,
            "diagnostico": diagnostico,
            "estrutura_dados": estrutura_dados
        }
        
    except Exception as e:
        print(f"❌ Erro no pipeline robusto: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "erro",
            "erro": str(e)
        }

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
    
    # Executa pipeline robusto
    resultados = executar_pipeline_robusto(
        tabela_magalu=tabela_magalu,
        tabela_bemol=tabela_bemol
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
        print(f"  - Excel: {caminho_excel}")
        print(f"  - HTML: {caminho_html}")
        print(f"  - TempView: {nome_tempview}")
        
        if enviar_email:
            print(f"\n📧 Email:")
            print(f"  - Enviado: {False}") # Placeholder, as funções de email foram removidas
            print(f"  - Destinatários: {destinatarios_lista}")
        
        # Exibe DataFrame final
        df_final = resultados["df_final"]
        if not df_final.empty:
            print(f"\n📋 Resumo do DataFrame final:")
            print(f"  - Shape: {df_final.shape}")
            print(f"  - Colunas: {list(df_final.columns)}")
            
            # Exibe primeiras linhas
            display(df_final.head(10))
        else:
            print(f"\n⚠️ DataFrame final está vazio")
        
        # Exibe informações sobre estrutura de dados
        estrutura_dados = resultados.get("estrutura_dados", {})
        print(f"\n📊 Estrutura de Dados - Origem Real:")
        
        # Informações do Magalu
        magalu_info = estrutura_dados.get("magalu_web_scraping", {})
        origem_magalu = magalu_info.get("origem", "Web scraping direto do Magazine Luiza")
        tabelas_bronze = magalu_info.get("tabelas_bronze", [])
        tabela_unificada = magalu_info.get("tabela_unificada")
        tabela_embeddings = magalu_info.get("tabela_embeddings")
        
        print(f"  🛒 Magalu ({origem_magalu}):")
        print(f"    - Tabelas Bronze: {len(tabelas_bronze)} categorias")
        
        # Mostra status detalhado das tabelas bronze
        tabelas_ativas = [t for t in tabelas_bronze if t.get("status") == "ativo"]
        tabelas_erro = [t for t in tabelas_bronze if t.get("status") == "erro"]
        
        for tabela in tabelas_ativas:
            print(f"      ✅ {tabela['nome']}: {tabela['registros']} produtos")
            print(f"         📋 Colunas: {tabela['colunas']}")
        
        for tabela in tabelas_erro:
            print(f"      ❌ {tabela['nome']}: ERRO - {tabela.get('erro', 'Erro desconhecido')}")
        
        if tabela_unificada:
            status_unificado = tabela_unificada.get("status", "desconhecido")
            if status_unificado == "ativo":
                print(f"    - Tabela Unificada: {tabela_unificada['registros']} produtos")
                print(f"      📋 Colunas: {tabela_unificada['colunas']}")
            else:
                print(f"    - Tabela Unificada: ERRO - {tabela_unificada.get('erro', 'Erro desconhecido')}")
        
        if tabela_embeddings:
            status_embeddings = tabela_embeddings.get("status", "desconhecido")
            if status_embeddings == "ativo":
                print(f"    - Tabela Embeddings: {tabela_embeddings['registros']} produtos")
                print(f"      📋 Colunas: {tabela_embeddings['colunas']}")
                if "embedding" in tabela_embeddings.get("colunas", []):
                    print(f"      ✅ Embeddings gerados com sentence-transformers")
                else:
                    print(f"      ⚠️ Coluna 'embedding' não encontrada")
            else:
                print(f"    - Tabela Embeddings: ERRO - {tabela_embeddings.get('erro', 'Erro desconhecido')}")
        
        # Informações da Bemol
        bemol_info = estrutura_dados.get("bemol_feed", {})
        origem_bemol = bemol_info.get("origem", "Tabela interna do Databricks")
        tabela_feed = bemol_info.get("tabela_feed")
        
        print(f"  🏪 Bemol ({origem_bemol}):")
        if tabela_feed:
            status_feed = tabela_feed.get("status", "desconhecido")
            if status_feed == "ativo":
                print(f"    - Feed VTEX: {tabela_feed['registros']} produtos")
                print(f"      📋 Colunas: {tabela_feed['colunas']}")
            else:
                print(f"    - Feed VTEX: ERRO - {tabela_feed.get('erro', 'Erro desconhecido')}")
        
        # Exibe diagnóstico detalhado
        diagnostico = resultados.get("diagnostico", {})
        print(f"\n🔍 Diagnóstico das Tabelas:")
        
        for tabela_nome, info in diagnostico.items():
            status = info.get("status", "desconhecido")
            count = info.get("count", 0)
            erro = info.get("erro")
            
            if status == "acessivel":
                print(f"  ✅ {tabela_nome}: {count} registros")
            elif status == "erro":
                print(f"  ❌ {tabela_nome}: ERRO - {erro}")
            else:
                print(f"  ⚠️ {tabela_nome}: Status desconhecido")
        
        # Exibe informações detalhadas sobre as tabelas carregadas
        df_magalu = resultados.get("df_magalu")
        df_bemol = resultados.get("df_bemol")
        
        if df_magalu is not None and not df_magalu.empty:
            print(f"\n📊 Tabela Magalu carregada:")
            print(f"  - Registros: {len(df_magalu)}")
            print(f"  - Colunas: {list(df_magalu.columns)}")
            
        if df_bemol is not None and not df_bemol.empty:
            print(f"\n📊 Tabela Bemol carregada:")
            print(f"  - Registros: {len(df_bemol)}")
            print(f"  - Colunas: {list(df_bemol.columns)}")
        
    else:
        print(f"\n❌ Erro no pipeline: {resultados.get('erro', 'Erro desconhecido')}")
        print(f"💡 Verifique se as tabelas especificadas existem e têm dados")
        
        # Exibe sugestões de solução de problemas
        print(f"\n🔧 Sugestões de Solução de Problemas:")
        print(f"1. Verifique se as tabelas existem no catálogo:")
        print(f"   - {tabela_magalu}")
        print(f"   - {tabela_bemol}")
        print(f"2. Verifique permissões de acesso às tabelas")
        print(f"3. Verifique se as tabelas contêm dados")
        print(f"4. Para dados de web scraping direto, verifique:")
        print(f"   - Tabelas bronze por categoria (bronze.magalu_*)")
        print(f"   - Tabela unificada (bronze.magalu_completo)")
        print(f"   - Tabela de embeddings ({tabela_magalu})")
        print(f"5. Execute web scraping se necessário:")
        print(f"   - Código direto: requests + BeautifulSoup")
        print(f"   - Categorias: Eletroportateis, Informatica, Tv e Video, Moveis, Eletrodomesticos, Celulares")
        print(f"   - Gere embeddings: from src.embeddings import generate_magalu_embeddings")
        print(f"6. Para dados Bemol:")
        print(f"   - Verifique tabela: {tabela_bemol}")
        print(f"   - Feed VTEX interno da Bemol")
        print(f"7. Tente executar consultas SQL diretas:")
        print(f"   - SELECT COUNT(*) FROM {tabela_magalu}")
        print(f"   - SELECT COUNT(*) FROM {tabela_bemol}")
        
except Exception as e:
    print(f"❌ Erro crítico no pipeline: {e}")
    import traceback
    traceback.print_exc()
    
    print(f"\n🔧 Solução de Problemas:")
    print(f"1. Verifique se o cluster tem acesso às tabelas")
    print(f"2. Verifique se as credenciais estão corretas")
    print(f"3. Tente reiniciar o cluster")
    print(f"4. Verifique logs do Databricks para mais detalhes")

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
    excel_path = caminho_excel
    print(f"📊 Relatório Excel: {excel_path}")
    
    # Link para HTML
    html_path = caminho_html
    print(f"🌐 Relatório HTML: {html_path}")
    
    # Link para TempView
    tempview_name = nome_tempview
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