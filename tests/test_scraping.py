import pytest
from src.scraping import run_scraping_pipeline

def test_run_scraping_pipeline(monkeypatch):
    # Mock logger para não logar durante o teste
    import src.logger_config
    src.logger_config.logger.info = lambda msg: None
    src.logger_config.logger.error = lambda msg: None
    # Mock requests.get se necessário
    # monkeypatch.setattr('requests.get', lambda *a, **k: type('r', (), {'content': b'', 'status_code': 200})())
    try:
        run_scraping_pipeline()
    except Exception:
        pytest.fail("run_scraping_pipeline() levantou exceção!") 