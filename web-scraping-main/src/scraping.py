from src.logger_config import logger
from src.config.settings import settings
import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict

def parse_magalu_products(search_term: str, max_pages: int = 5) -> pd.DataFrame:
    """
    Realiza scraping de produtos do Magazine Luiza para um termo de busca.
    """
    base_url = settings.SCRAPING_CONFIG["magazine_luiza"]["base_url"]
    headers = {"User-Agent": settings.USER_AGENT}
    produtos: List[Dict] = []
    
    for page in range(1, max_pages + 1):
        url = f"{base_url}{search_term}/?page={page}"
        logger.info(f"Scraping Magalu: {url}")
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            cards = soup.select('div[data-testid="product-card-content"]')
            for card in cards:
                try:
                    title = card.select_one('h2').text.strip()
                    price_elem = card.select_one('p[data-testid="price-value"]')
                    price = price_elem.text.strip() if price_elem else None
                    link_element = card.find_parent('a')
                    product_url = link_element['href'] if link_element else None
                    produtos.append({
                        "title": title,
                        "price": price,
                        "url": product_url
                    })
                except Exception as e:
                    logger.warning(f"Erro ao extrair produto: {e}")
        except Exception as e:
            logger.error(f"Erro ao acessar página {url}: {e}")
    return pd.DataFrame(produtos)

def run_scraping_pipeline() -> None:
    """
    Executa o scraping apenas do Magazine Luiza e salva os dados.
    """
    try:
        logger.info("Iniciando scraping do Magazine Luiza...")
        # Exemplo: buscar por 'notebook' (pode parametrizar via widget)
        df = parse_magalu_products(search_term="notebook", max_pages=5)
        if not df.empty:
            output_path = settings.SCRAPING_CONFIG["magazine_luiza"]["table_name"] + ".csv"
            df.to_csv(output_path, index=False)
            logger.info(f"Scraping finalizado. {len(df)} produtos salvos em {output_path}")
        else:
            logger.warning("Nenhum produto encontrado no Magazine Luiza.")
    except Exception as e:
        logger.error(f"Erro no scraping do Magazine Luiza: {e}")
        raise 