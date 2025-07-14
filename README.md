# Web Scraping Benchmarking

Este projeto realiza benchmarking de produtos entre marketplaces (ex: Magalu, Bemol) utilizando web scraping, processamento de dados, embeddings de linguagem natural e matching inteligente para comparação de preços e produtos.

## Estrutura do Projeto

```
webscrapingmkt/
│
├── notebooks/           # Notebooks de orquestração (Databricks)
├── src/                 # Módulos Python reutilizáveis (scraping, processamento, matching, relatórios)
├── tests/               # Testes unitários (pytest)
├── config.py            # Configurações centralizadas
├── requirements.txt     # Dependências do projeto
└── README.md            # Documentação
```

## Principais Funcionalidades
- Web scraping de múltiplos marketplaces
- Processamento e limpeza de dados
- Geração de embeddings com Sentence Transformers
- Matching de produtos via similaridade de embeddings
- Relatórios em Excel e HTML
- Envio automático de e-mails com resultados

## Como usar
1. **Configuração:**
   - Ajuste `config.py` com parâmetros de ambiente, caminhos e segredos (use dbutils.secrets.get() no Databricks).
2. **Instalação de dependências:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Execução:**
   - Use os notebooks em `notebooks/` para orquestrar o pipeline.
   - Os módulos em `src/` podem ser usados em scripts ou notebooks.
4. **Testes:**
   ```bash
   pytest tests/
   ```

## Observações
- **Nunca** coloque segredos diretamente no código.
- O projeto é compatível com Databricks (uso de widgets, display, spark.table, etc).
- Logging estruturado via `src/logger_config.py`.

## Contribuição
Pull requests são bem-vindos! Siga o padrão de modularização e inclua testes para novas funcionalidades. 