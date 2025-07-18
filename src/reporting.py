import pandas as pd
from typing import Optional, Tuple
from .logger_config import get_logger

logger = get_logger(__name__)


def exportar_para_excel(
    df: pd.DataFrame, 
    caminho_arquivo: str,
    colunas_ocultas: Optional[list] = None
) -> str:
    """
    Exporta DataFrame para arquivo Excel, removendo colunas técnicas.
    
    Args:
        df: DataFrame para exportar
        caminho_arquivo: Caminho do arquivo Excel
        colunas_ocultas: Lista de colunas para ocultar no relatório
        
    Returns:
        str: Caminho do arquivo exportado
    """
    try:
        from .data_processing import definir_colunas_relatorio
        
        # Define colunas visíveis e ocultas
        if colunas_ocultas is None:
            colunas_visiveis, _ = definir_colunas_relatorio(df)
        else:
            colunas_visiveis = [col for col in df.columns if col not in colunas_ocultas]
        
        # Exporta apenas colunas visíveis
        df_export = df[colunas_visiveis].copy()
        df_export.to_excel(caminho_arquivo, index=False)
        
        logger.info(f"Relatório Excel exportado: {caminho_arquivo}")
        logger.info(f"Colunas exportadas: {len(colunas_visiveis)}")
        
        return caminho_arquivo
        
    except Exception as e:
        logger.error(f"Erro ao exportar para Excel: {e}")
        raise


def exportar_relatorio_benchmarking_excel(
    df_final: pd.DataFrame,
    caminho_excel: str = "benchmarking_completo.xlsx"
) -> str:
    """
    Exporta relatório de benchmarking no formato exato do Excel mostrado na imagem.
    
    Args:
        df_final: DataFrame com dados processados
        caminho_excel: Caminho do arquivo Excel
        
    Returns:
        str: Caminho do arquivo Excel exportado
    """
    try:
        # Define colunas para o relatório (formato exato da imagem)
        colunas_relatorio = [
            "title",           # Nome do produto
            "marketplace",     # Bemol ou Magalu
            "price",          # Preço
            "url",            # Link do produto
            "exclusividade",  # sim ou não
            "diferenca_percentual"  # Diferença percentual
        ]
        
        # Filtra apenas colunas que existem no DataFrame
        colunas_existentes = [col for col in colunas_relatorio if col in df_final.columns]
        
        # Cria DataFrame para exportação
        df_export = df_final[colunas_existentes].copy()
        
        # Formata preços para formato brasileiro
        if "price" in df_export.columns:
            df_export["price"] = df_export["price"].apply(
                lambda x: f"{x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".") 
                if pd.notnull(x) and x != 0 else ""
            )
        
        # Formata diferença percentual
        if "diferenca_percentual" in df_export.columns:
            df_export["diferenca_percentual"] = df_export["diferenca_percentual"].apply(
                lambda x: f"{x:.2f}%" if pd.notnull(x) and x != 0 else ""
            )
        
        # Exporta para Excel
        df_export.to_excel(caminho_excel, index=False, sheet_name="benchmarking_produtos")
        
        logger.info(f"Relatório Excel de benchmarking exportado: {caminho_excel}")
        logger.info(f"Total de produtos: {len(df_export)}")
        logger.info(f"Produtos pareados: {len(df_export[df_export['exclusividade'] == 'não'])}")
        logger.info(f"Produtos exclusivos: {len(df_export[df_export['exclusividade'] == 'sim'])}")
        
        return caminho_excel
        
    except Exception as e:
        logger.error(f"Erro ao exportar relatório Excel: {e}")
        raise


def gerar_relatorio_html(
    df: pd.DataFrame,
    caminho_html: str = "/dbfs/FileStore/relatorio_comparativo.html",
    colunas_ocultas: Optional[list] = None
) -> str:
    """
    Gera relatório HTML com estilo personalizado.
    
    Args:
        df: DataFrame para exportar
        caminho_html: Caminho do arquivo HTML
        colunas_ocultas: Lista de colunas para ocultar no relatório
        
    Returns:
        str: Caminho do arquivo HTML gerado
    """
    try:
        from .data_processing import definir_colunas_relatorio
        
        # Define colunas visíveis e ocultas
        if colunas_ocultas is None:
            colunas_visiveis, _ = definir_colunas_relatorio(df)
        else:
            colunas_visiveis = [col for col in df.columns if col not in colunas_ocultas]
        
        # Prepara DataFrame para HTML
        df_export = df[colunas_visiveis].copy()
        
        # Define o estilo HTML da tabela
        style_html = """
        <style>
            body {
                width: 100%;
                font-size: 14px;
                font-family: Calibri, Arial, sans-serif;
            }

            table {
                border: 0;
                border-collapse: collapse;
                table-layout: fixed;
                font-size: 1.2em;
                width: 100%;
                white-space: nowrap;
            }

            th {
                white-space: nowrap;
                width: 10%;
                text-align: center;
                background-color: #87CEEB;
                padding: 8px;
                border: 1px solid #ddd;
            }

            td {
                white-space: nowrap;
                text-align: center;
                padding: 6px;
                border: 1px solid #ddd;
            }

            @media screen and (min-width: 900px) {
                body {
                    font-size: 16px;
                }
            }
        </style>
        """
        
        # Converte a tabela para HTML com estilo
        html_tabela = style_html + df_export.to_html(index=False, escape=False)
        
        # Salva o arquivo HTML
        dbutils.fs.put(f"dbfs:{caminho_html.replace('/dbfs', '')}", html_tabela, overwrite=True)
        
        logger.info(f"Relatório HTML exportado: {caminho_html}")
        logger.info(f"Colunas exportadas: {len(colunas_visiveis)}")
        
        return caminho_html
        
    except Exception as e:
        logger.error(f"Erro ao exportar para HTML: {e}")
        raise


def criar_tempview_databricks(
    df: pd.DataFrame, 
    nome_tempview: str = "tempview_benchmarking_pares"
) -> str:
    """
    Cria TempView no Databricks para consultas SQL.
    
    Args:
        df: DataFrame para criar TempView
        nome_tempview: Nome da TempView
        
    Returns:
        str: Nome da TempView criada
    """
    try:
        # Converte para Spark DataFrame e cria TempView
        spark_df = spark.createDataFrame(df)
        spark_df.createOrReplaceTempView(nome_tempview)
        
        logger.info(f"TempView criada: {nome_tempview}")
        return nome_tempview
        
    except Exception as e:
        logger.error(f"Erro ao criar TempView: {e}")
        raise


def gerar_relatorio_benchmarking(
    df_final: pd.DataFrame,
    caminho_excel: Optional[str] = None,
    caminho_html: Optional[str] = None
) -> Tuple[str, str, str]:
    """
    Gera relatório completo de benchmarking de produtos.
    
    Args:
        df_final: DataFrame com dados processados
        caminho_excel: Caminho opcional para arquivo Excel
        caminho_html: Caminho opcional para arquivo HTML
        
    Returns:
        Tuple: (caminho_excel, caminho_html, nome_tempview)
    """
    try:
        # Cria TempView para consultas SQL
        nome_tempview = criar_tempview_databricks(df_final)
        
        # Define caminhos padrão se não fornecidos
        if caminho_excel is None:
            caminho_excel = "benchmarking_completo.xlsx"
        if caminho_html is None:
            caminho_html = "/dbfs/FileStore/relatorio_comparativo.html"
        
        # Exporta para Excel (formato exato da imagem)
        caminho_exportado_excel = exportar_relatorio_benchmarking_excel(df_final, caminho_excel)
        
        # Exporta para HTML
        caminho_exportado_html = gerar_relatorio_html(df_final, caminho_html)
        
        # Log de resumo
        total_produtos = len(df_final)
        produtos_exclusivos = len(df_final[df_final["exclusividade"] == "sim"])
        produtos_pareados = total_produtos - produtos_exclusivos
        
        logger.info(f"Relatório gerado com sucesso:")
        logger.info(f"- Total de produtos: {total_produtos}")
        logger.info(f"- Produtos pareados: {produtos_pareados}")
        logger.info(f"- Produtos exclusivos: {produtos_exclusivos}")
        logger.info(f"- Arquivo Excel: {caminho_exportado_excel}")
        logger.info(f"- Arquivo HTML: {caminho_exportado_html}")
        logger.info(f"- TempView SQL: {nome_tempview}")
        
        return caminho_exportado_excel, caminho_exportado_html, nome_tempview
        
    except Exception as e:
        logger.error(f"Erro ao gerar relatório: {e}")
        raise


def obter_estatisticas_relatorio(df: pd.DataFrame) -> dict:
    """
    Obtém estatísticas do relatório de benchmarking.
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        dict: Estatísticas do relatório
    """
    try:
        stats = {
            "total_produtos": len(df),
            "produtos_magalu": len(df[df["marketplace"] == "Magalu"]),
            "produtos_bemol": len(df[df["marketplace"] == "Bemol"]),
            "produtos_exclusivos": len(df[df["exclusividade"] == "sim"]),
            "produtos_pareados": len(df[df["exclusividade"] == "não"]),
            "muito_similar": len(df[df["nivel_similaridade"] == "muito similar"]),
            "moderadamente_similar": len(df[df["nivel_similaridade"] == "moderadamente similar"]),
            "pouco_similar": len(df[df["nivel_similaridade"] == "pouco similar"]),
            "exclusivo": len(df[df["nivel_similaridade"] == "exclusivo"])
        }
        
        # Calcula preços médios
        if len(df) > 0:
            stats["preco_medio_magalu"] = df[df["marketplace"] == "Magalu"]["price"].mean()
            stats["preco_medio_bemol"] = df[df["marketplace"] == "Bemol"]["price"].mean()
        
        logger.info("Estatísticas do relatório calculadas")
        return stats
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas: {e}")
        raise


def preparar_dados_email(df_final: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    Prepara dados para envio de email, filtrando produtos exclusivos do Magalu.
    
    Args:
        df_final: DataFrame com dados processados
        
    Returns:
        Tuple: (DataFrame_exclusivos_magalu, HTML_tabela)
    """
    try:
        # Filtra exclusivos Magalu para corpo do e-mail
        df_exclusivos_magalu = df_final[
            (df_final["exclusividade"] == "sim") &
            (df_final["marketplace"] == "Magalu")
        ].reset_index(drop=True)
        
        # Formata preço
        df_exclusivos_magalu["price"] = pd.to_numeric(df_exclusivos_magalu["price"], errors="coerce")
        df_exclusivos_magalu["price"] = df_exclusivos_magalu["price"].apply(
            lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        )
        
        # Gera HTML para o corpo do e-mail
        df_ia_insights_html = df_exclusivos_magalu.to_html(index=False, escape=False)
        
        logger.info(f"Dados preparados para email: {len(df_exclusivos_magalu)} produtos exclusivos Magalu")
        return df_exclusivos_magalu, df_ia_insights_html
        
    except Exception as e:
        logger.error(f"Erro ao preparar dados para email: {e}")
        raise


def criar_anexo_excel(df_final: pd.DataFrame) -> object:
    """
    Cria anexo Excel em memória para envio por email.
    
    Args:
        df_final: DataFrame com dados completos
        
    Returns:
        object: Objeto Attachment do SendGrid
    """
    try:
        import base64
        from io import BytesIO
        from sendgrid.helpers.mail import Attachment, FileContent, FileName, FileType, Disposition
        
        # Gera Excel em memória
        excel_buffer = BytesIO()
        df_final.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        encoded_file = base64.b64encode(excel_buffer.read()).decode()
        
        # Cria attachment
        attachment = Attachment(
            FileContent(encoded_file),
            FileName("benchmarking_completo.xlsx"),
            FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            Disposition("attachment")
        )
        
        logger.info("Anexo Excel criado com sucesso")
        return attachment
        
    except Exception as e:
        logger.error(f"Erro ao criar anexo Excel: {e}")
        raise


def enviar_email_relatorio(
    df_final: pd.DataFrame,
    stats: dict,
    caminho_excel: str,
    caminho_html: str,
    destinatarios: Optional[list] = None,
    assunto: Optional[str] = None,
    remetente: str = "caiomiguel@bemol.com.br",
    api_key: Optional[str] = None
) -> bool:
    """
    Envia relatório por email usando SendGrid com formatação personalizada.
    
    Args:
        df_final: DataFrame com dados processados
        stats: Estatísticas do relatório
        caminho_excel: Caminho do arquivo Excel
        caminho_html: Caminho do arquivo HTML
        destinatarios: Lista de emails destinatários
        assunto: Assunto do email
        remetente: Email do remetente
        api_key: API key do SendGrid (opcional)
        
    Returns:
        bool: True se email foi enviado com sucesso
    """
    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, From
        from datetime import datetime
        
        # Obtém configurações do SendGrid
        if api_key is None:
            try:
                api_key = dbutils.secrets.get(scope="sendgrid", key="api_key")
            except Exception:
                logger.error("API key do SendGrid não encontrada. Configure o secret 'sendgrid/api_key' no Databricks.")
                return False
        
        # Define destinatários padrão se não fornecidos
        if destinatarios is None:
            try:
                to_emails = dbutils.secrets.get(scope="sendgrid", key="to_emails").split(",")
            except Exception:
                to_emails = ["renatobolf@bemol.com.br"]
        else:
            to_emails = destinatarios
        
        # Define assunto padrão se não fornecido
        if assunto is None:
            data_atual = datetime.now().strftime('%Y-%m-%d')
            assunto = f"Scraping - Benchmarking de produtos - {data_atual}"
        
        # Prepara dados para email
        df_exclusivos_magalu, df_ia_insights_html = preparar_dados_email(df_final)
        
        # Cria anexo Excel
        attachment = criar_anexo_excel(df_final)
        
        # Corpo HTML do e-mail
        style_html = """<style>
            body { width: 100%; font-size: 14px; font-family: Calibri, Arial, sans-serif; }
            table { border: 0; border-collapse: collapse; table-layout: fixed; font-size: 1.2em; width: 100%; white-space: nowrap; }
            th { white-space: nowrap; width: 10%; text-align: center; background-color: #87CEEB; padding: 8px; border: 1px solid #ddd; }
            td { white-space: nowrap; text-align: center; padding: 6px; border: 1px solid #ddd; }
            @media screen and (min-width: 900px) { body { font-size: 16px; } }
        </style>"""
        
        html = style_html + f"""
        <p style="font-size: 12pt;">Prezados,</p>
        <p style="font-size: 12pt;">Bom dia,</p>
        <p style="font-size: 12pt;">Compartilho o benchmarking realizado hoje.</p>
        <br>{df_ia_insights_html}<br>
        <p style="color: red; font-size: 12pt;">(Este e-mail foi enviado automaticamente por um código Python)</p><br>
        <span style="font-size: 12pt;">Atenciosamente,</span><br>
        <b style="color: #0B5394; font-size: 12pt;">Dados Marketing</b><br>
        <img src="https://static.bemol.com.br/mails/email-mkt/qualidade/Assinatura-Qualidade-Varejo.gif"><br>
        """
        
        # Monta o e-mail
        message = Mail(
            from_email=From(remetente),
            to_emails=to_emails,
            subject=assunto,
            html_content=html
        )
        message.add_bcc("caiomiguel@bemol.com.br")
        message.add_attachment(attachment)
        
        # Envia e-mail
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        
        logger.info(f"Email enviado com sucesso! Status: {response.status_code}")
        logger.info(f"Destinatários: {to_emails}")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao enviar email: {e}")
        return False


def enviar_email_simples(message, log_email: str, api_key: str) -> bool:
    """
    Função auxiliar para envio de email simples.
    
    Args:
        message: Objeto Mail do SendGrid
        log_email: Email para log
        api_key: API key do SendGrid
        
    Returns:
        bool: True se email foi enviado com sucesso
    """
    try:
        from sendgrid import SendGridAPIClient
        
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        logger.info(f"✅ E-mail enviado para: {log_email} | Status: {response.status_code}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao enviar: {e}")
        return False 