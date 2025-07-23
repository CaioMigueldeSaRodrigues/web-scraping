import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import time
from typing import Dict, List

def _clean_price(price_str: str) -> float:
    """
    Limpa uma string de preço, removendo símbolos e formatação,
    e a converte para float. Retorna 0.0 se a conversão falhar.
    """
    if not isinstance(price_str, str):
        return 0.0
    cleaned_str = re.sub(r'[^\d,.]', '', price_str)
    cleaned_str = cleaned_str.replace('.', '').replace(',', '.')
    try:
        if cleaned_str.count('.') > 1:
            parts = cleaned_str.split('.')
            cleaned_str = "".join(parts[:-1]) + "." + parts[-1]
        return float(cleaned_str)
    except (ValueError, TypeError):
        return 0.0

def scrape_category(config: Dict, headers: Dict) -> pd.DataFrame:
    """
    Realiza o scraping de uma categoria específica de um marketplace.
    """
    marketplace_name = config['marketplace_name']
    category = config['category']
    base_url = config['base_url']
    selectors = config['selectors']
    
    print(f"--- Iniciando scraping: {marketplace_name} - {category} ---")
    products_data = []

    for page in range(1, config['max_pages'] + 1):
        url = base_url.format(page)
        print(f"Raspando página {page}: {url}")

        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            product_list = soup.select(selectors['product_card'])

            if not product_list:
                print(f"Nenhum produto encontrado na página {page}. Encerrando busca para esta categoria.")
                break

            for product in product_list:
                title_tag = product.select_one(selectors['title'])
                price_tag = product.select_one(selectors['price'])
                link_tag = product.select_one(selectors['link'])

                title = title_tag.get_text(strip=True) if title_tag else None
                price_str = price_tag.get_text(strip=True) if price_tag else '0'
                
                if link_tag and link_tag.has_attr('href'):
                    link_href = link_tag['href']
                    domain = config.get('domain_for_relative_urls')
                    link = f"{domain}{link_href}" if link_href.startswith('/') and domain else link_href
                else:
                    link = None

                price = _clean_price(price_str)

                if title and price > 0 and link:
                    products_data.append({
                        'title': title,
                        'price': price,
                        'url': link,
                        'marketplace': marketplace_name,
                        'category': category
                    })
            
            time.sleep(1.5)

        except requests.RequestException as e:
            print(f"ERRO: Falha de rede na página {page} de {category}. Erro: {e}")
            break
        except Exception as e:
            print(f"ERRO: Falha de parsing na página {page} de {category}. Erro: {e}")

    print(f"--- Scraping de {category} concluído. Total: {len(products_data)} produtos. ---")
    return pd.DataFrame(products_data) 