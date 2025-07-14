# src/config/settings.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Manages application settings and configurations."""

    # USER_AGENT mantido aqui explicitamente devido à ausência de Key Vault.
    # TODO: Mover para Databricks Secrets quando a infraestrutura estiver disponível.
    USER_AGENT: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )

    SCRAPING_CONFIG: dict = {
        "magazine_luiza": {
            "base_url": "https://www.magazineluiza.com.br/busca/",
            "table_name": "bronze_scraping.magazine_luiza_products"
        }
    }
    APP_NAME: str = "WebScrapingPipeline"

settings = Settings() 