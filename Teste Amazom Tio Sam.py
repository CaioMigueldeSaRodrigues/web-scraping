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
import random
import pytesseract
from PIL import Image

# Configura o log
logging.basicConfig(level=logging.INFO)

# API e headers
API_KEY = "40d1181bf8b55f690e805a8338e9df32"
SCRAPER_API_URL = "http://api.scraperapi.com"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"
]

HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"
}

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

def extract_product_links(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    pattern = re.compile(r"/dp/[A-Z0-9]{10}")
    urls = [f"https://www.amazon.com.br{tag['href']}" for tag in soup.find_all("a", href=pattern)]
    return list(set(urls))

def solve_captcha(chrome):
    try:
        img = WebDriverWait(chrome, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "img"))
        )
        src = img.get_attribute("src")
        chrome.save_screenshot("captcha_page.png")
        location = img.location
        size = img.size
        image = Image.open("captcha_page.png")
        left = location['x']
        top = location['y']
        right = left + size['width']
        bottom = top + size['height']
        image = image.crop((left, top, right, bottom))
        image.save("captcha.png")
        captcha_text = pytesseract.image_to_string(image).strip()
        return captcha_text
    except Exception as e:
        logging.warning(f"Falha ao tentar resolver o CAPTCHA: {e}")
        return None

def extract_product_info_selenium(urls: list):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    chrome = webdriver.Chrome(options=chrome_options)

    products = []
    for i, url in enumerate(urls, 1):
        try:
            logging.info(f"({i}/{len(urls)}) Acessando: {url}")
            chrome.get(url)
            time.sleep(random.uniform(2.5, 4.5))

            if "captcha" in chrome.current_url.lower():
                captcha_code = solve_captcha(chrome)
                if captcha_code:
                    input_box = chrome.find_element(By.ID, "captchacharacters")
                    input_box.send_keys(captcha_code)
                    submit_button = chrome.find_element(By.CLASS_NAME, "a-button-text")
                    submit_button.click()
                    time.sleep(5)

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
        time.sleep(random.uniform(2.5, 4.5))

    chrome.quit()
    return products

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
