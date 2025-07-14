# Web Scraping Benchmarking

Este projeto realiza benchmarking de produtos entre marketplaces (ex: Magalu) utilizando web scraping, processamento de dados, embeddings de linguagem natural e matching inteligente para comparação de preços e produtos.

## Estrutura do Projeto

```
webscrapingmkt/
│
├── notebooks/           # Notebooks de orquestração (Databricks)
│   └── benchmarking_orchestration.py  # Notebook principal de orquestração (Databricks)
├── src/                 # Módulos Python reutilizáveis (scraping, processamento, matching, relatórios)
├── tests/               # Testes unitários (pytest)
├── config.py            # Configurações centralizadas
├── pyproject.toml       # Gerenciamento de dependências (Poetry)
├── ruff.toml            # Configuração de lint/format (Ruff)
├── .github/workflows/   # Workflows de CI (GitHub Actions)
└── README.md            # Documentação
```

## Principais Funcionalidades
- Web scraping do Magazine Luiza
- Processamento e limpeza de dados
- Geração de embeddings com Sentence Transformers
- Matching de produtos via similaridade de embeddings
- Relatórios em Excel e HTML
- Envio automático de e-mails com resultados
- Qualidade de código automatizada (Ruff, Mypy, CI)

## Como usar

### 1. Instalação das dependências
Utilize o [Poetry](https://python-poetry.org/):
```bash
pip install poetry
poetry install
```

> **Nota:** O projeto depende de `pydantic-settings`, `requests`, `beautifulsoup4` e outras bibliotecas listadas no `pyproject.toml`. Todas são instaladas automaticamente pelo Poetry.

### 2. Lint, formatação e checagem de tipos
```bash
poetry run ruff check .           # Linting
poetry run ruff format .          # Formatação
poetry run mypy src               # Checagem de tipos
```

### 3. Execução dos testes
```bash
poetry run pytest
```

### 4. Orquestração
- Use o notebook `notebooks/benchmarking_orchestration.py` para orquestrar o pipeline no Databricks.
- Os módulos em `src/` podem ser usados em scripts ou notebooks.

## Integração Contínua (CI)
O projeto utiliza GitHub Actions para:
- Instalar dependências
- Rodar lint, format, type-check e testes em Python 3.9 e 3.10
- Workflow: `.github/workflows/ci-pipeline.yml`

## Observações
- **Nunca** coloque segredos diretamente no código.
- O projeto é compatível com Databricks (uso de widgets, display, spark.table, etc).
- Logging estruturado via `src/logger_config.py`.

## Contribuição
Pull requests são bem-vindos! Siga o padrão de modularização, inclua testes e mantenha a qualidade de código (Ruff/Mypy). 