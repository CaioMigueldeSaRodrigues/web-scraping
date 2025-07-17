import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64
import io
import logging
from src.config import SENDGRID_API_KEY, FROM_EMAIL, TO_EMAILS, EMAIL_SUBJECT

def generate_excel_report(df: pd.DataFrame) -> bytes:
    logging.info("Gerando relatório em Excel...")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Comparativo_Precos')
    return output.getvalue()

def generate_html_report(df: pd.DataFrame) -> str:
    logging.info("Gerando corpo do email em HTML...")
    exclusives_df = df[df['exclusivo'] == True].copy()
    
    html = """
    <html><head><style>
        body { font-family: sans-serif; }
        h1, h2 { color: #333; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        a { color: #0066cc; }
    </style></head><body>
    <h1>Relatório de Análise de Concorrência</h1>
    """
    
    if not exclusives_df.empty:
        html += f"<h2>{len(exclusives_df)} Produtos Exclusivos Encontrados no Concorrente</h2>"
        html += exclusives_df[['produto_site', 'preco_site', 'categoria_site', 'url_site']].to_html(index=False, render_links=True, escape=False)
    else:
        html += "<h2>Nenhum produto exclusivo encontrado.</h2>"
        
    html += "<p>O relatório completo com todos os produtos comparados está em anexo.</p>"
    html += "</body></html>"
    return html

def send_email_report(html_content: str, excel_attachment: bytes):
    if not SENDGRID_API_KEY:
        logging.error("API Key do SendGrid não configurada. Email não será enviado.")
        return

    logging.info(f"Enviando email para: {', '.join(TO_EMAILS)}")
    message = Mail(from_email=FROM_EMAIL, to_emails=TO_EMAILS, subject=EMAIL_SUBJECT, html_content=html_content)
    encoded_file = base64.b64encode(excel_attachment).decode()
    attachedFile = Attachment(
        FileContent(encoded_file),
        FileName('relatorio_concorrencia.xlsx'),
        FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        Disposition('attachment')
    )
    message.attachment = attachedFile
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logging.info(f"Email enviado com sucesso. Status Code: {response.status_code}")
    except Exception as e:
        logging.error(f"Falha ao enviar email via SendGrid: {e}") 