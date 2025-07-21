# Databricks notebook source

import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64
import io
import logging

def _generate_excel(df: pd.DataFrame, sheet_name: str, column_widths: dict = None) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        if column_widths:
            worksheet = writer.sheets[sheet_name]
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
    return output.getvalue()

# Função principal do módulo
def execute_reporting(analytical_df: pd.DataFrame, business_df: pd.DataFrame, config: dict):
    """Orquestra a geração de relatórios e o envio de email."""
    logging.info("--- INICIANDO ETAPA DE GERAÇÃO DE RELATÓRIOS E NOTIFICAÇÃO ---")

    # 1. Geração dos Relatórios Excel
    business_excel = _generate_excel(
        business_df, 
        'Comparativo_Negocios',
        {'A': 70, 'B': 15, 'C': 12, 'D': 70, 'E': 15, 'F': 22}
    )
    analytical_excel = _generate_excel(
        analytical_df.sort_values(by='similaridade', ascending=False),
        'Analise_Similaridade'
    )
    
    # 2. Geração do Corpo do Email
    exclusives_df = business_df[business_df['exclusividade'] == "Sim"]
    html_content = "<h1>Relatório de Análise de Concorrência</h1>"
    if not exclusives_df.empty:
        html_content += f"<h2>{len(exclusives_df)} Produtos Exclusivos Encontrados no Concorrente</h2>"
        html_content += exclusives_df[['title', 'price', 'url']].to_html(index=False, render_links=True, escape=False)
    else:
        html_content += "<h2>Nenhum produto exclusivo encontrado.</h2>"
    html_content += "<p>Os relatórios completos (negócios e analítico) estão em anexo.</p>"
    
    # 3. Envio do Email
    if not config['SENDGRID_API_KEY']:
        logging.error("API Key do SendGrid não configurada. Email não será enviado.")
        return

    message = Mail(from_email=config['FROM_EMAIL'], to_emails=config['TO_EMAILS'], subject=config['EMAIL_SUBJECT'], html_content=html_content)
    message.add_attachment(FileContent(base64.b64encode(business_excel).decode()), FileName('relatorio_negocios.xlsx'), FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'), Disposition('attachment'))
    message.add_attachment(FileContent(base64.b64encode(analytical_excel).decode()), FileName('relatorio_analitico_debug.xlsx'), FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'), Disposition('attachment'))

    try:
        SendGridAPIClient(config['SENDGRID_API_KEY']).send(message)
        logging.info("Email enviado com sucesso.")
    except Exception as e:
        logging.error(f"Falha ao enviar email: {e}") 