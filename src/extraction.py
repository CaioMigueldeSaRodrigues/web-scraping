# Módulo de extração de dados (Web Scraping)
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import time
from typing import Dict

def _clean_price(price_str: str) -> float:
    if not isinstance(price_str, str): return 0.0
    cleaned_str = re.sub(r'[^\d,.]', '', price_str).replace('.', '').replace(',', '.')
    if cleaned_str.count('.') > 1:
        parts = cleaned_str.split('.')
        cleaned_str = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(cleaned_str)
    except (ValueError, TypeError):
        return 0.0

def scrape_category(config: Dict, headers: Dict) -> pd.DataFrame:
    marketplace_name = config['marketplace_name']
    category = config['category']
    base_url = config['base_url']
    selectors = config['selectors']
    
    print(f"--- Iniciando extração: {marketplace_name} - {category} ---")
    products_data = []

    for page in range(1, config['max_pages'] + 1):
        url = base_url.format(page)
        print(f"Extraindo da página {page}: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            product_list = soup.select(selectors['product_card'])

            if not product_list:
                print(f"Nenhum produto na página {page}. Encerrando para esta categoria.")
                break

            for product in product_list:
                title = product.select_one(selectors['title']).get_text(strip=True) if product.select_one(selectors['title']) else None
                price_str = product.select_one(selectors['price']).get_text(strip=True) if product.select_one(selectors['price']) else '0'
                link_tag = product.select_one(selectors['link'])
                if link_tag and link_tag.has_attr('href'):
                    link_href = link_tag['href']
                    domain = config.get('domain_for_relative_urls')
                    link = f"{domain}{link_href}" if link_href.startswith('/') and domain else link_href
                else:
                    link = None

                if title and link:
                    products_data.append({
                        'title': title, 'price': _clean_price(price_str), 'url': link,
                        'marketplace': marketplace_name, 'category': category
                    })
            time.sleep(1.5)
        except requests.RequestException as e:
            print(f"ERRO DE REDE na página {page} de {category}: {e}")
            break
        except Exception as e:
            print(f"ERRO DE PARSING na página {page} de {category}: {e}")

    print(f"--- Extração de {category} concluída. Total: {len(products_data)} produtos. ---")
    return pd.DataFrame(products_data) 