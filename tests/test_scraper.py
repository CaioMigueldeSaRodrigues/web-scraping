# tests/test_scraper.py
import pytest
from unittest.mock import MagicMock
from bs4 import BeautifulSoup

from src.scraping.scraper import Scraper

@pytest.fixture
def scraper_instance():
    """Fornece uma instância do Scraper para os testes."""
    return Scraper(user_agent="Test-Agent/1.0")

@pytest.fixture
def mock_magalu_html():
    """Fornece um HTML mockado para uma página de resultados da Magazine Luiza."""
    return """
    <div>
      <ul data-testid="product-list">
        <li data-testid="product-card">
          <h2 data-testid="product-title">Produto Teste Magalu 1</h2>
          <p data-testid="price-value">R$ 4.500,00</p>
        </li>
        <li data-testid="product-card">
          <h2 data-testid="product-title">Produto Teste Magalu 2</h2>
          <p data-testid="price-value">R$ 5.200,00</p>
        </li>
      </ul>
    </div>
    """

def test_parse_magazine_luiza(scraper_instance, mock_magalu_html):
    """
    Testa o parser da Magazine Luiza com HTML mockado para garantir a extração correta.
    """
    soup = BeautifulSoup(mock_magalu_html, 'html.parser')
    # Testando o método privado
    result = scraper_instance._Scraper__parse_magazine_luiza(soup)
    
    assert len(result) == 2
    assert result[0] == {'name': 'Produto Teste Magalu 1', 'price': 'R$ 4.500,00'}
    assert result[1] == {'name': 'Produto Teste Magalu 2', 'price': 'R$ 5.200,00'}

def test_scrape_dispatches_to_magalu_parser(scraper_instance, mock_magalu_html):
    """
    Garante que o método scrape chama o parser correto da Magazine Luiza.
    """
    scraper_instance._Scraper__get_html = MagicMock(return_value=mock_magalu_html)
    scraper_instance._Scraper__parse_magazine_luiza = MagicMock(return_value=[{"parsed": True}])

    scraper_instance.scrape(site_name="magazine_luiza", url="http://fake.magalu.url")
    
    scraper_instance._Scraper__parse_magazine_luiza.assert_called_once() 