import base64
import io
import os
import re
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, util
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Attachment,
    Content,
    Disposition,
    FileContent,
    FileName,
    FileType,
    Mail,
)

# --- CONFIGURAÇÃO E CONSTANTES ---
load_dotenv()
SIMILARITY_THRESHOLD = 0.85
MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'
DATABRICKS_TABLE = "bol.feed_varejo_vtex" # Tabela interna para comparação

# Categorias a serem extraídas do Magazine Luiza
CATEGORIAS_MAGALU = {
    "Eletroportateis": "https://www.magazineluiza.com.br/eletroportateis/l/ep/?page={}",
    "Informatica": "https://www.magazineluiza.com.br/informatica/l/in/?page={}",
    "Tv e Video": "https://www.magazineluiza.com.br/tv-e-video/l/et/?page={}",
    "Moveis": "https://www.magazineluiza.com.br/moveis/l/mo/?page={}",
    "Eletrodomesticos": "https://www.magazineluiza.com.br/eletrodomesticos/l/ed/?page={}",
    "Celulares": "https://www.magazineluiza.com.br/celulares-e-smartphones/l/te/?page={}"
}

# --- 1. SETUP AMBIENTE (Já realizado) ---

def setup_spark_session():
    """Inicializa e retorna uma sessão Spark."""
    return SparkSession.builder.appName("PriceComparator").getOrCreate()

# --- 2. EXTRAÇÃO DE DADOS ---

def scrape_magalu_products(categories: dict, pages_per_category: int = 5) -> pd.DataFrame:
    """
    Faz scraping de múltiplas categorias do Magazine Luiza, consolidando em um único DataFrame.
    Integra a lógica do script original.
    """
    print("Iniciando scraping multi-categoria do Magazine Luiza...")
    all_products = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    for category_name, base_url in categories.items():
        for page in range(1, pages_per_category + 1):
            url = base_url.format(page)
            print(f"Extraindo dados de: [{category_name}] - Página {page}")
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"Erro ao acessar a URL {url}: {e}. Pulando para a próxima.")
                break # Se uma página falha, provavelmente as seguintes também falharão

            soup = BeautifulSoup(response.content, 'html.parser')
            product_cards = soup.select('div[data-testid="product-card-content"]')

            if not product_cards:
                print(f"Nenhum produto encontrado na página {page} de {category_name}. Finalizando esta categoria.")
                break

            for card in product_cards:
                try:
                    title = card.select_one('h2').text.strip()
                    price_elem = card.select_one('p[data-testid="price-value"]')
                    
                    if price_elem:
                        price_str = re.sub(r'[^\d,]', '', price_elem.text.strip()).replace(',', '.')
                        price = float(price_str)
                    else:
                        continue # Pula produtos sem preço

                    link_element = card.find_parent('a')
                    product_url = "https://www.magazineluiza.com.br" + link_element['href'] if link_element and link_element.has_attr('href') else ""
                    
                    all_products.append({
                        'titulo_site': title,
                        'preco_site': price,
                        'url_site': product_url,
                        'categoria_site': category_name
                    })
                except (AttributeError, ValueError, TypeError) as e:
                    # Silenciosamente ignora cards malformados para não poluir o log
                    continue
            time.sleep(1) # Respeita o servidor

    print(f"\nScraping concluído. Total de {len(all_products)} produtos extraídos de todas as categorias.")
    return pd.DataFrame(all_products)

def load_databricks_data(spark: SparkSession, table_name: str) -> pd.DataFrame:
    """Carrega dados de uma tabela do Databricks e retorna como DataFrame Pandas."""
    print(f"Carregando dados da tabela Databricks: {table_name}")
    try:
        df_spark = spark.sql(f"""
            SELECT id, title as titulo_tabela, price as preco_tabela, link as url_tabela
            FROM {table_name}
            WHERE price > 0 AND title IS NOT NULL
        """)
        return df_spark.toPandas()
    except Exception as e:
        print(f"Erro ao carregar dados do Databricks: {e}")
        return pd.DataFrame(columns=['id', 'titulo_tabela', 'preco_tabela', 'url_tabela'])

# --- 3. PRÉ-PROCESSAMENTO E EMBEDDINGS ---

def generate_embeddings(df: pd.DataFrame, column_name: str, model) -> pd.DataFrame:
    """Gera embeddings para os títulos e armazena em uma nova coluna."""
    print(f"Gerando embeddings para a coluna '{column_name}'...")
    if df.empty or column_name not in df.columns:
        df['embedding'] = None
        return df
        
    titles = df[column_name].tolist()
    embeddings = model.encode(titles, convert_to_tensor=True, show_progress_bar=True)
    df['embedding'] = list(embeddings.cpu().numpy())
    return df

# --- 4. COMPARAÇÃO E SIMILARIDADE ---

def find_best_matches(df_site: pd.DataFrame, df_table: pd.DataFrame) -> tuple:
    """Calcula a similaridade de cosseno e encontra os melhores matches."""
    print("Calculando similaridade e encontrando matches...")
    if df_site.empty or df_table.empty:
        return [], df_site.to_dict('records')

    embeddings_site = df_site['embedding'].tolist()
    embeddings_table = df_table['embedding'].tolist()
    cosine_scores = util.cos_sim(embeddings_site, embeddings_table)

    matches = []
    matched_site_indices = set()

    for i in range(len(embeddings_site)):
        best_match_score, best_match_idx = cosine_scores[i].max(dim=0)
        
        if best_match_score.item() >= SIMILARITY_THRESHOLD:
            site_product = df_site.iloc[i]
            table_product = df_table.iloc[best_match_idx]
            
            matches.append({
                'produto_site': site_product['titulo_site'],
                'produto_tabela': table_product['titulo_tabela'],
                'similaridade': best_match_score.item(),
                'preco_site': site_product['preco_site'],
                'preco_tabela': table_product['preco_tabela'],
                'url_site': site_product['url_site'],
                'url_tabela': table_product['url_tabela'],
                'categoria_site': site_product.get('categoria_site', 'N/A')
            })
            matched_site_indices.add(i)

    exclusive_indices = set(range(len(df_site))) - matched_site_indices
    exclusive_products = [df_site.iloc[i].to_dict() for i in exclusive_indices]
    
    print(f"Processamento concluído. {len(matches)} matches encontrados. {len(exclusive_products)} produtos exclusivos.")
    return matches, exclusive_products

# --- 5. INDEXAÇÃO DOS RESULTADOS ---

def create_final_table(matches: list, exclusives: list) -> pd.DataFrame:
    """Cria a tabela final com diferenciais de preço e produtos exclusivos."""
    print("Criando tabela de resultados...")
    df_matches = pd.DataFrame(matches)
    
    if not df_matches.empty:
        df_matches['diferencial_percentual'] = ((df_matches['preco_site'] - df_matches['preco_tabela']) / df_matches['preco_tabela']) * 100
        df_matches = df_matches.sort_values(by='similaridade', ascending=False)

    df_exclusives = pd.DataFrame(exclusives)
    if not df_exclusives.empty:
        df_exclusives = df_exclusives.rename(columns={'titulo_site': 'produto_site', 'preco_site': 'preco_site', 'url_site': 'url_site', 'categoria_site': 'categoria_site'})
        df_exclusives['produto_tabela'] = 'EXCLUSIVO'
        df_exclusives['similaridade'] = 0
        df_exclusives['preco_tabela'] = 0
        df_exclusives['diferencial_percentual'] = 100
        df_exclusives['url_tabela'] = ''

    final_cols = ['produto_site', 'produto_tabela', 'similaridade', 'preco_site', 'preco_tabela', 'diferencial_percentual', 'url_site', 'url_tabela', 'categoria_site']
    df_final = pd.concat([df_matches.reindex(columns=final_cols), df_exclusives.reindex(columns=final_cols, fill_value='')], ignore_index=True)
    return df_final

# --- 6. GERAR OUTPUTS ---

def generate_outputs(df_final: pd.DataFrame, exclusives: list) -> tuple[str, str]:
    """Gera a planilha Excel e o HTML para o email."""
    print("Gerando arquivos de output (Excel e HTML)...")
    excel_path = "comparativo_precos.xlsx"
    df_final.to_excel(excel_path, index=False, engine='openpyxl')

    html_content = "<html><head><style>body{font-family: sans-serif;} table{border-collapse: collapse; width: 100%;} th, td{border: 1px solid #dddddd; text-align: left; padding: 8px;} tr:nth-child(even){background-color: #f2f2f2;}</style></head>"
    html_content += "<body><h2>Produtos Exclusivos Encontrados no Site</h2><p>Estes produtos foram encontrados no site do Magazine Luiza, mas não possuem um correspondente com similaridade alta em nossa base de dados.</p>"
    if exclusives:
        html_content += "<table><tr><th>Produto</th><th>Categoria</th><th>Preço</th><th>Link</th></tr>"
        for prod in exclusives:
            html_content += f"<tr><td>{prod['titulo_site']}</td><td>{prod['categoria_site']}</td><td>R$ {prod['preco_site']:.2f}</td><td><a href='{prod['url_site']}'>Ver produto</a></td></tr>"
        html_content += "</table>"
    else:
        html_content += "<p>Nenhum produto exclusivo encontrado nesta execução.</p>"
    html_content += "</body></html>"
    return excel_path, html_content

# --- 7. ENVIO DE EMAIL ---

def send_email_with_attachment(html_content: str, attachment_path: str):
    """Envia email via SendGrid com anexo e corpo HTML."""
    print("Preparando e enviando email via SendGrid...")
    sg_key, email_from, email_to = os.getenv("SENDGRID_API_KEY"), os.getenv("EMAIL_FROM"), os.getenv("EMAIL_TO")
    if not all([sg_key, email_from, email_to]):
        print("Variáveis de ambiente do SendGrid não configuradas. Email não será enviado.")
        return

    message = Mail(
        from_email=email_from, to_emails=email_to.split(','),
        subject=f"Comparativo de Preços e Produtos Exclusivos - {datetime.now().strftime('%Y-%m-%d')}",
        html_content=html_content)

    with open(attachment_path, 'rb') as f: data = f.read()
    encoded_file = base64.b64encode(data).decode()
    attachedFile = Attachment(FileContent(encoded_file), FileName(os.path.basename(attachment_path)),
                              FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'), Disposition('attachment'))
    message.attachment = attachedFile

    try:
        sg = SendGridAPIClient(sg_key)
        response = sg.send(message)
        print(f"Email enviado com sucesso! Status Code: {response.status_code}")
    except Exception as e:
        print(f"Erro ao enviar email: {e}")

# --- 8. DASH (Preparação de Dados) ---

def prepare_data_for_bi(df_final: pd.DataFrame):
    """Salva os dados processados na pasta KPI_BI para consumo pelo dashboard."""
    print("Preparando e salvando dados para o BI...")
    bi_path = "KPI_BI"
    os.makedirs(bi_path, exist_ok=True)
    df_final.to_csv(os.path.join(bi_path, "dados_comparativo.csv"), index=False)
    # A documentação README.md já foi criada na fase anterior e permanece válida.
    print(f"Dados salvos em '{os.path.join(bi_path, 'dados_comparativo.csv')}'.")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # 1. Setup
    # spark = setup_spark_session() # Descomente se for rodar em ambiente com Spark Connect
    
    # 2. Extração
    # Reduzido para 2 páginas por categoria para um teste rápido. Aumente conforme necessário.
    df_site = scrape_magalu_products(CATEGORIAS_MAGALU, pages_per_category=2) 
    
    # --- Mock de dados da tabela interna para teste local ---
    mock_data = {
        'id': [101, 102, 103, 104, 105],
        'titulo_tabela': [
            'Notebook Gamer Acer Nitro 5 AN515-57-57XQ Intel Core i5 8GB', 
            'Smartphone Samsung Galaxy S23 5G 128GB Tela 6.1"',
            'Smart TV 50" UHD 4K Samsung 50CU7700',
            'Air Fryer Oven de Bancada 12L Digital Oster',
            'Guarda-Roupa Casal 3 Portas de Correr com Espelho'
        ],
        'preco_tabela': [4500.00, 3899.90, 2189.00, 750.00, 1200.00],
        'url_tabela': ['http://example.com/101', 'http://example.com/102', 'http://example.com/103', 'http://example.com/104', 'http://example.com/105']
    }
    df_databricks = pd.DataFrame(mock_data)
    # --- Fim do mock ---
    # Para produção, use: df_databricks = load_databricks_data(spark, DATABRICKS_TABLE)

    if df_site.empty:
        print("Pipeline interrompida: Falha no scraping, nenhum produto foi extraído.")
    else:
        # 3. Embeddings
        model = SentenceTransformer(MODEL_NAME)
        df_site_embedded = generate_embeddings(df_site.copy(), 'titulo_site', model)
        df_databricks_embedded = generate_embeddings(df_databricks.copy(), 'titulo_tabela', model)

        # 4. Comparação
        matches_list, exclusives_list = find_best_matches(df_site_embedded, df_databricks_embedded)
        
        # 5. Indexação
        df_final_results = create_final_table(matches_list, exclusives_list)
        
        # 6. Outputs
        excel_file, html_body = generate_outputs(df_final_results, exclusives_list)
        
        # 7. Envio de Email
        send_email_with_attachment(html_body, excel_file)

        # 8. Preparação para BI
        prepare_data_for_bi(df_final_results)

        print("\nPipeline executada com sucesso.") 