# src/main.py
# Este é o script principal que será executado pelo Job no Databricks.

import pandas as pd
from typing import Optional, Tuple, Dict, Any
from .logger_config import get_logger
from .embeddings import processar_embeddings_completos
from .reporting import (
    gerar_relatorio_benchmarking,
    obter_estatisticas_relatorio,
    enviar_email_relatorio
)

logger = get_logger(__name__)


def listar_tabelas_disponiveis() -> dict:
    """
    Lista todas as tabelas disponíveis no catálogo para debug.
    
    Returns:
        dict: Informações sobre as tabelas disponíveis
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
        logger.error(f"❌ Erro ao listar tabelas: {e}")
        return {}


def validar_parametros_pipeline(
    tabela_magalu: str,
    tabela_bemol: str
) -> bool:
    """
    Valida parâmetros do pipeline antes da execução.
    
    Args:
        tabela_magalu: Nome da tabela do Magalu
        tabela_bemol: Nome da tabela da Bemol
        
    Returns:
        bool: True se parâmetros são válidos
    """
    try:
        logger.info(f"🔍 Validando parâmetros do pipeline...")
        logger.info(f"Tabela Magalu: {tabela_magalu}")
        logger.info(f"Tabela Bemol: {tabela_bemol}")
        
        # Verifica se as tabelas existem
        try:
            tabelas_existentes = spark.catalog.listTables()
            nomes_tabelas = [table.name for table in tabelas_existentes]
            logger.info(f"Tabelas disponíveis: {nomes_tabelas}")
            
            if tabela_magalu not in nomes_tabelas:
                logger.error(f"❌ Tabela {tabela_magalu} não encontrada")
                logger.error(f"Tabelas disponíveis: {nomes_tabelas}")
                return False
                
            if tabela_bemol not in nomes_tabelas:
                logger.error(f"❌ Tabela {tabela_bemol} não encontrada")
                logger.error(f"Tabelas disponíveis: {nomes_tabelas}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro ao listar tabelas: {e}")
            return False
        
        # Verifica se as tabelas têm dados
        try:
            logger.info(f"📊 Verificando dados da tabela {tabela_magalu}")
            count_magalu = spark.table(tabela_magalu).count()
            logger.info(f"Tabela {tabela_magalu}: {count_magalu} registros")
            
            if count_magalu == 0:
                logger.error(f"❌ Tabela {tabela_magalu} está vazia")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro ao verificar tabela {tabela_magalu}: {e}")
            return False
            
        try:
            logger.info(f"📊 Verificando dados da tabela {tabela_bemol}")
            count_bemol = spark.table(tabela_bemol).count()
            logger.info(f"Tabela {tabela_bemol}: {count_bemol} registros")
            
            if count_bemol == 0:
                logger.error(f"❌ Tabela {tabela_bemol} está vazia")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro ao verificar tabela {tabela_bemol}: {e}")
            return False
        
        # Verifica se as tabelas têm as colunas necessárias
        try:
            logger.info("🔍 Verificando estrutura das tabelas...")
            
            # Verifica tabela Magalu
            df_magalu_sample = spark.table(tabela_magalu).limit(1).toPandas()
            colunas_necessarias = ["title", "price", "url", "embedding"]
            colunas_faltantes = [col for col in colunas_necessarias if col not in df_magalu_sample.columns]
            
            if colunas_faltantes:
                logger.error(f"❌ Tabela {tabela_magalu} está faltando colunas: {colunas_faltantes}")
                logger.info(f"Colunas disponíveis: {list(df_magalu_sample.columns)}")
                return False
                
            # Verifica tabela Bemol
            df_bemol_sample = spark.table(tabela_bemol).limit(1).toPandas()
            colunas_faltantes = [col for col in colunas_necessarias if col not in df_bemol_sample.columns]
            
            if colunas_faltantes:
                logger.error(f"❌ Tabela {tabela_bemol} está faltando colunas: {colunas_faltantes}")
                logger.info(f"Colunas disponíveis: {list(df_bemol_sample.columns)}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro ao verificar estrutura das tabelas: {e}")
            return False
        
        logger.info(f"✅ Validação concluída com sucesso:")
        logger.info(f"  - {tabela_magalu}: {count_magalu} produtos")
        logger.info(f"  - {tabela_bemol}: {count_bemol} produtos")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro geral na validação de parâmetros: {e}")
        return False


def executar_pipeline_benchmarking(
    tabela_magalu: str = "silver.embeddings_magalu_completo",
    tabela_bemol: str = "silver.embeddings_bemol"
) -> Tuple[pd.DataFrame, str, str, str]:
    """
    Executa pipeline completo de benchmarking de produtos.
    
    Args:
        tabela_magalu: Nome da tabela do Magalu
        tabela_bemol: Nome da tabela da Bemol
        
    Returns:
        Tuple: (DataFrame_final, caminho_excel, caminho_html, nome_tempview)
    """
    try:
        logger.info("🚀 Iniciando pipeline de benchmarking")
        
        # Valida parâmetros
        logger.info("🔍 Iniciando validação de parâmetros...")
        if not validar_parametros_pipeline(tabela_magalu, tabela_bemol):
            error_msg = f"❌ Validação de parâmetros falhou. Verifique as tabelas de entrada:"
            error_msg += f"\n  - Tabela Magalu: {tabela_magalu}"
            error_msg += f"\n  - Tabela Bemol: {tabela_bemol}"
            error_msg += f"\n\nVerifique se:"
            error_msg += f"\n  1. As tabelas existem no catálogo"
            error_msg += f"\n  2. As tabelas contêm dados"
            error_msg += f"\n  3. As tabelas têm as colunas necessárias: title, price, url, embedding"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Carrega dados das tabelas
        logger.info("📊 Carregando dados das tabelas")
        df_magalu = spark.table(tabela_magalu).toPandas()
        df_bemol = spark.table(tabela_bemol).toPandas()
        
        logger.info(f"Dados carregados: Magalu ({len(df_magalu)} produtos), Bemol ({len(df_bemol)} produtos)")
        
        # Processa embeddings e similaridade
        logger.info("🔍 Processando embeddings e similaridade")
        df_final = processar_embeddings_completos(df_magalu, df_bemol)
        
        # Gera relatórios
        logger.info("📋 Gerando relatórios")
        caminho_excel, caminho_html, nome_tempview = gerar_relatorio_benchmarking(df_final)
        
        # Calcula estatísticas
        stats = obter_estatisticas_relatorio(df_final)
        
        logger.info("✅ Pipeline de benchmarking concluído com sucesso")
        logger.info(f"📊 Estatísticas: {stats}")
        
        return df_final, caminho_excel, caminho_html, nome_tempview
        
    except Exception as e:
        logger.error(f"❌ Erro no pipeline de benchmarking: {e}")
        raise


def executar_pipeline_completo(
    tabela_magalu: str = "silver.embeddings_magalu_completo",
    tabela_bemol: str = "silver.embeddings_bemol",
    caminho_excel: Optional[str] = None,
    caminho_html: Optional[str] = None,
    nome_tempview: Optional[str] = None
) -> Dict[str, Any]:
    """
    Executa pipeline completo com geração de relatórios.
    
    Args:
        tabela_magalu: Nome da tabela do Magalu
        tabela_bemol: Nome da tabela da Bemol
        caminho_excel: Caminho opcional para arquivo Excel
        caminho_html: Caminho opcional para arquivo HTML
        nome_tempview: Nome opcional para TempView
        
    Returns:
        Dict: Resultados do pipeline
    """
    try:
        # Executa pipeline de benchmarking
        df_final, excel_path, html_path, tempview_name = executar_pipeline_benchmarking(
            tabela_magalu, tabela_bemol
        )
        
        # Calcula estatísticas
        stats = obter_estatisticas_relatorio(df_final)
        
        # Prepara resultados
        resultados = {
            "df_final": df_final,
            "caminho_excel": excel_path,
            "caminho_html": html_path,
            "nome_tempview": tempview_name,
            "estatisticas": stats,
            "status": "sucesso"
        }
        
        logger.info("🎉 Pipeline completo executado com sucesso")
        return resultados
        
    except Exception as e:
        logger.error(f"❌ Erro no pipeline completo: {e}")
        return {
            "status": "erro",
            "erro": str(e)
        }


def executar_pipeline_com_email(
    tabela_magalu: str = "silver.embeddings_magalu_completo",
    tabela_bemol: str = "silver.embeddings_bemol",
    destinatarios_email: Optional[list] = None,
    assunto_email: Optional[str] = None,
    remetente: str = "caiomiguel@bemol.com.br"
) -> Dict[str, Any]:
    """
    Executa pipeline completo com envio de email.
    
    Args:
        tabela_magalu: Nome da tabela do Magalu
        tabela_bemol: Nome da tabela da Bemol
        destinatarios_email: Lista de emails destinatários
        assunto_email: Assunto do email
        remetente: Email do remetente
        
    Returns:
        Dict: Resultados do pipeline
    """
    try:
        # Executa pipeline de benchmarking
        df_final, excel_path, html_path, tempview_name = executar_pipeline_benchmarking(
            tabela_magalu, tabela_bemol
        )
        
        # Calcula estatísticas
        stats = obter_estatisticas_relatorio(df_final)
        
        # Envia email
        logger.info("📧 Enviando relatório por email")
        email_enviado = enviar_email_relatorio(
            df_final=df_final,
            stats=stats,
            caminho_excel=excel_path,
            caminho_html=html_path,
            destinatarios=destinatarios_email,
            assunto=assunto_email,
            remetente=remetente
        )
        
        # Prepara resultados
        resultados = {
            "df_final": df_final,
            "caminho_excel": excel_path,
            "caminho_html": html_path,
            "nome_tempview": tempview_name,
            "estatisticas": stats,
            "email_enviado": email_enviado,
            "status": "sucesso"
        }
        
        logger.info("🎉 Pipeline com email executado com sucesso")
        return resultados
        
    except Exception as e:
        logger.error(f"❌ Erro no pipeline com email: {e}")
        return {
            "status": "erro",
            "erro": str(e)
        }


def executar_pipeline_completo_com_email(
    tabela_magalu: str = "silver.embeddings_magalu_completo",
    tabela_bemol: str = "silver.embeddings_bemol",
    caminho_excel: Optional[str] = None,
    caminho_html: Optional[str] = None,
    nome_tempview: Optional[str] = None,
    enviar_email: bool = False,
    destinatarios_email: Optional[list] = None,
    assunto_email: Optional[str] = None,
    remetente: str = "caiomiguel@bemol.com.br"
) -> Dict[str, Any]:
    """
    Executa pipeline completo com opção de envio de email.
    
    Args:
        tabela_magalu: Nome da tabela do Magalu
        tabela_bemol: Nome da tabela da Bemol
        caminho_excel: Caminho opcional para arquivo Excel
        caminho_html: Caminho opcional para arquivo HTML
        nome_tempview: Nome opcional para TempView
        enviar_email: Se deve enviar email
        destinatarios_email: Lista de emails destinatários
        assunto_email: Assunto do email
        remetente: Email do remetente
        
    Returns:
        Dict: Resultados do pipeline
    """
    try:
        if enviar_email:
            return executar_pipeline_com_email(
                tabela_magalu=tabela_magalu,
                tabela_bemol=tabela_bemol,
                destinatarios_email=destinatarios_email,
                assunto_email=assunto_email,
                remetente=remetente
            )
        else:
            return executar_pipeline_completo(
                tabela_magalu=tabela_magalu,
                tabela_bemol=tabela_bemol,
                caminho_excel=caminho_excel,
                caminho_html=caminho_html,
                nome_tempview=nome_tempview
            )
            
    except Exception as e:
        logger.error(f"❌ Erro no pipeline completo com email: {e}")
        return {
            "status": "erro",
            "erro": str(e)
        } 