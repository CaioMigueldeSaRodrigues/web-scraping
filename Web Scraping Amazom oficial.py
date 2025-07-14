import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import re
import logging
import pandas as pd
import time

# Configuração do log
logging.basicConfig(level=logging.INFO)

# ScraperAPI e cabeçalhos7
API_KEY = "40d1181bf8b55f690e805a8338e9df32"
SCRAPER_API_URL = "http://api.scraperapi.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"
}

# Função para obter o HTML da página de resultados da Amazon via API
def fetch_html_via_api(url: str):
    try:
        params = {
            'api_key': API_KEY,
            'url': url,
            'render': 'true',
            'keep_headers': 'true'
        }
        response = requests.get(SCRAPER_API_URL, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logging.error(f"Erro ao buscar a URL {url}: {e}")
        return None

# Função para extrair URLs de produtos da Amazon na página de resultados
def extract_product_links(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    urls = []
    pattern = re.compile(r"/dp/[A-Z0-9]{10}")
    for tag in soup.find_all("a", href=pattern):
        url = f"https://www.amazon.com.br{tag['href']}"
        urls.append(url)
    return list(set(urls))  # Remove duplicatas

# Função que acessa cada URL com Selenium e extrai os dados dos produtos
def extract_product_info_selenium(urls: list):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0")
    chrome = webdriver.Chrome(options=chrome_options)

    products = []

    for i, url in enumerate(urls, 1):
        try:
            logging.info(f"({i}/{len(urls)}) Acessando: {url}")
            chrome.get(url)
            time.sleep(3)

            # Verifica se há desafio de CAPTCHA
            if "captcha" in chrome.current_url.lower():
                logging.warning(f"Recaptcha detectado em: {url}")

                try:
                    input_box = WebDriverWait(chrome, 10).until(
                        EC.presence_of_element_located((By.ID, "captchacharacters"))
                    )
                    captcha_code = input("Digite o texto da imagem CAPTCHA visível no navegador: ")
                    input_box.send_keys(captcha_code)

                    submit_button = chrome.find_element(By.CLASS_NAME, "a-button-text")
                    submit_button.click()

                    WebDriverWait(chrome, 10).until(
                        EC.presence_of_element_located((By.ID, "productTitle"))
                    )

                except Exception as e:
                    logging.warning(f"Falha ao lidar com CAPTCHA: {e}")

            produto = WebDriverWait(chrome, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="productTitle"]'))
            ).text.strip()

            try:
                preco = WebDriverWait(chrome, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'a-price-whole'))
                ).text.strip()
            except TimeoutException:
                preco = "Indisponível"

            try:
                asin = re.search(r"/([A-Z0-9]{10})(?:[/?]|$)", chrome.current_url).group(1)
            except:
                asin = None

            try:
                marca_elemento = chrome.find_element(By.XPATH, "//*[contains(text(), 'Marca') or contains(text(),'Brand')]/following-sibling::*[1]")
                marca = marca_elemento.text.strip()
            except NoSuchElementException:
                marca = None

            products.append({
                "title": produto,
                "price": preco,
                "asin": asin,
                "brand": marca,
                "url": url
            })

        except Exception as e:
            logging.warning(f"Erro ao processar {url}: {e}")
        time.sleep(2)

    chrome.quit()
    return products

# Execução principal com visualização ativa
if __name__ == "__main__":
    html = fetch_html_via_api("https://www.amazon.com.br/s?k=ofertas+e+promo%C3%A7%C3%B5es")
    if html:
        links = extract_product_links(html)
        print(f"Total de links encontrados: {len(links)}")
        produtos = extract_product_info_selenium(links)

        if produtos:
            df = pd.DataFrame(produtos)
            df.to_excel("ofertas_amazon.xlsx", index=False)
            print("\nResumo das extrações:")
            print(df.head())
        else:
            print("Nenhum dado extraído com sucesso.")
    else:
        print("Erro ao buscar HTML inicial.")
