# src/config/settings.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Manages application settings and configurations."""
    USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    APP_NAME: str = "WebScrapingPipeline"
    
    # Configuração centralizada para as categorias
    CATEGORIES: dict = {
        "Eletroportateis": "https://www.magazineluiza.com.br/eletroportateis/l/ep/?page={}",
        "Informatica": "https://www.magazineluiza.com.br/informatica/l/in/?page={}",
        "Tv e Video": "https://www.magazineluiza.com.br/tv-e-video/l/et/?page={}",
        "Moveis": "https://www.magazineluiza.com.br/moveis/l/mo/?page={}",
        "Eletrodomesticos": "https://www.magazineluiza.com.br/eletrodomesticos/l/ed/?page={}",
        "Celulares": "https://www.magazineluiza.com.br/celulares-e-smartphones/l/te/?page={}"
    }
    
    # Tabela Delta única para todos os produtos
    DELTA_TABLE_PATH: str = "bronze.magazine_luiza_products"

settings = Settings() 