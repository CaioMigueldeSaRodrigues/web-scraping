# src/scraping/scraper.py
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from src.logger_config import logger

class Scraper:
    """Handles web scraping operations specifically for Magazine Luiza."""

    def __init__(self, user_agent: str):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self.__parsers = {
            "magazine_luiza": self.__parse_magazine_luiza,
        }

    def __get_html(self, url: str) -> Optional[str]:
        """Fetches HTML content from a URL, handling errors."""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except RequestException as e:
            logger.error(f"Failed to retrieve HTML from {url}. Error: {e}")
            return None

    def __parse_magazine_luiza(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parses product information from Magazine Luiza HTML soup."""
        products_list: List[Dict[str, Any]] = []
        # Selector para os cards de produto da Magazine Luiza
        results = soup.find_all('li', attrs={'data-testid': 'product-card'})
        
        for product in results:
            name_tag = product.find('h2', attrs={'data-testid': 'product-title'})
            price_tag = product.find('p', attrs={'data-testid': 'price-value'})
            
            if name_tag and price_tag:
                name = name_tag.text.strip()
                price = price_tag.text.strip()
                products_list.append({'name': name, 'price': price})
        return products_list

    def scrape(self, site_name: str, url: str) -> List[Dict[str, Any]]:
        """Main scraping method that dispatches to the correct parser."""
        logger.info(f"Scraping {site_name} from URL: {url}")
        
        parser_func = self.__parsers.get(site_name)
        if not parser_func:
            logger.warning(f"No parser available for site: {site_name}")
            return []

        html_content = self.__get_html(url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        products = parser_func(soup)
            
        logger.info(f"Successfully parsed {len(products)} products from {site_name}.")
        return products 