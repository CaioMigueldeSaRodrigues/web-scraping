# Benchmarking de Concorrência - Bemol vs Magalu

Este projeto automatiza a extração, comparação e análise de preços entre a Bemol (VTEX) e o concorrente Magazine Luiza, utilizando scraping, NLP e relatórios automatizados para BI e áreas de negócio.

## Visão Geral
- **Scraping**: Coleta de produtos e preços do site Magalu por categoria.
- **Processamento**: Geração de embeddings e matching de produtos usando NLP.
- **Relatórios**: Geração de outputs analíticos (para BI) e de negócios (Excel/Email).
- **Automação**: Pipeline orquestrado para rodar em Databricks ou localmente.

## Estrutura do Projeto
```
├── src/
│   ├── config.py           # Configurações e secrets
│   ├── scraping.py         # Scraping e ingestão de dados
│   ├── data_processing.py  # Embeddings, matching e formatação
│   ├── reporting.py        # Geração de relatórios e envio de email
│   └── main.py             # Orquestração do pipeline
├── tests/                  # Testes unitários
├── requirements.txt        # Dependências do projeto
├── .gitignore              # Arquivos a serem ignorados pelo git
├── README.md               # Este arquivo
```

## Como Executar
### 1. Instale as dependências
```bash
pip install -r requirements.txt
```

### 2. Configure os secrets
- No Databricks: configure o secret scope `bemol-data-secrets` com a chave `SendGridAPI`.
- Localmente: defina a variável de ambiente `DB_BEMOL_DATA_SECRETS_SENDGRIDAPI` com sua chave SendGrid.

### 3. Execute o pipeline
- **Local:**
  ```bash
  python run.py
  ```
- **Databricks:**
  - Importe os arquivos para um notebook e execute `from src.main import run_pipeline; run_pipeline()`

## Principais Dependências
- Python 3.9+
- PySpark
- Pandas
- requests, BeautifulSoup4
- sentence-transformers, scikit-learn
- sendgrid

## Observações
- O pipeline salva dados intermediários em tabelas Delta (camada bronze) e lê a tabela VTEX do Databricks.
- O envio de email depende da configuração correta do SendGrid.

## Contato
Dúvidas ou sugestões: caiomiguel@bemol.com.br 