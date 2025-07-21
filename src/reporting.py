import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64
import io
import logging
from src.config import SENDGRID_API_KEY, FROM_EMAIL, TO_EMAILS, EMAIL_SUBJECT

def generate_business_report_excel(df: pd.DataFrame) -> bytes:
    """Gera o relatório Excel formatado para negócios (formato longo)."""
    logging.info("Gerando relatório de negócios em Excel...")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Comparativo_Concorrencia')
        workbook = writer.book
        worksheet = writer.sheets['Comparativo_Concorrencia']
        worksheet.column_dimensions['A'].width = 70
        worksheet.column_dimensions['B'].width = 15
        worksheet.column_dimensions['C'].width = 12
        worksheet.column_dimensions['D'].width = 70
        worksheet.column_dimensions['E'].width = 15
        worksheet.column_dimensions['F'].width = 22
    return output.getvalue()

def generate_analytical_report_excel(df: pd.DataFrame) -> bytes:
    """Gera um relatório Excel analítico (formato largo) para depuração."""
    logging.info("Gerando relatório analítico de depuração em Excel...")
    output = io.BytesIO()
    # Mostra os produtos com maior similaridade primeiro, mesmo que não atinjam o threshold
    df_sorted = df.sort_values(by='similaridade', ascending=False)
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_sorted.to_excel(writer, index=False, sheet_name='Analise_Similaridade')
    return output.getvalue()

def generate_html_report(df: pd.DataFrame) -> str:
    """Gera um HTML focado nos produtos exclusivos."""
    logging.info("Gerando corpo do email em HTML...")
    exclusives_df = df[df['exclusividade'] == "Sim"].copy()
    html = "<h1>Relatório de Análise de Concorrência</h1>"
    if not exclusives_df.empty:
        html += f"<h2>{len(exclusives_df)} Produtos Exclusivos Encontrados no Concorrente (Magalu)</h2>"
        html += exclusives_df[['title', 'price', 'url']].to_html(index=False, render_links=True, escape=False)
    else:
        html += "<h2>Nenhum produto exclusivo encontrado.</h2>"
    html += "<p>O relatório completo para o negócio e um relatório analítico para depuração estão em anexo.</p>"
    return html

def send_email_report(html_content: str, business_excel: bytes, analytical_excel: bytes):
    if not SENDGRID_API_KEY:
        logging.error("API Key do SendGrid não configurada. Email não será enviado.")
        return

    logging.info(f"Enviando email para: {', '.join(TO_EMAILS)}")
    message = Mail(from_email=FROM_EMAIL, to_emails=TO_EMAILS, subject=EMAIL_SUBJECT, html_content=html_content)
    
    # Anexo 1: Relatório de Negócios
    encoded_business = base64.b64encode(business_excel).decode()
    message.add_attachment(
        FileContent(encoded_business),
        FileName('relatorio_negocios.xlsx'),
        FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        Disposition('attachment')
    )

    # Anexo 2: Relatório Analítico
    encoded_analytical = base64.b64encode(analytical_excel).decode()
    message.add_attachment(
        FileContent(encoded_analytical),
        FileName('relatorio_analitico_debug.xlsx'),
        FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        Disposition('attachment')
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logging.info(f"Email enviado com sucesso. Status Code: {response.status_code}")
    except Exception as e:
        logging.error(f"Falha ao enviar email via SendGrid: {e}") 