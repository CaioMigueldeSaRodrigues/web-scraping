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
        # Verifica se as tabelas existem
        tabelas_existentes = spark.catalog.listTables()
        nomes_tabelas = [table.name for table in tabelas_existentes]
        
        if tabela_magalu not in nomes_tabelas:
            logger.error(f"Tabela {tabela_magalu} não encontrada")
            return False
            
        if tabela_bemol not in nomes_tabelas:
            logger.error(f"Tabela {tabela_bemol} não encontrada")
            return False
        
        # Verifica se as tabelas têm dados
        count_magalu = spark.table(tabela_magalu).count()
        count_bemol = spark.table(tabela_bemol).count()
        
        if count_magalu == 0:
            logger.error(f"Tabela {tabela_magalu} está vazia")
            return False
            
        if count_bemol == 0:
            logger.error(f"Tabela {tabela_bemol} está vazia")
            return False
        
        logger.info(f"Validação concluída: {tabela_magalu} ({count_magalu} produtos), {tabela_bemol} ({count_bemol} produtos)")
        return True
        
    except Exception as e:
        logger.error(f"Erro na validação de parâmetros: {e}")
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
        if not validar_parametros_pipeline(tabela_magalu, tabela_bemol):
            raise ValueError("Parâmetros inválidos para o pipeline")
        
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