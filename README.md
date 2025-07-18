# Pipeline de Benchmarking - Magalu vs Bemol

Pipeline completo de análise de concorrência entre Magalu e Bemol, implementado para o ambiente Databricks.

## 🚀 Funcionalidades

### ✅ Implementado
- **Extração de dados** das tabelas silver com embeddings
- **Cálculo de similaridade** usando cosine_similarity
- **Identificação de produtos exclusivos** por marketplace
- **Análise de diferença de preços** entre pares similares
- **Classificação automática** de níveis de similaridade
- **Geração de relatórios Excel** limpos para negócios
- **Criação de TempViews** para consultas SQL
- **Logging robusto** e tratamento de erros
- **Testes unitários** completos
- **Configuração parametrizável** via widgets

### 📊 Níveis de Similaridade
- **Exclusivo**: Produto único no marketplace
- **Muito Similar**: ≥ 85% de similaridade
- **Moderadamente Similar**: ≥ 50% de similaridade
- **Pouco Similar**: < 50% de similaridade

## 🏗️ Arquitetura

```
src/
├── main.py              # Orquestração principal do pipeline
├── data_processing.py   # Processamento de dados e limpeza
├── embeddings.py        # Cálculo de similaridade e matching
├── reporting.py         # Geração de relatórios
├── config.py           # Configurações centralizadas
└── logger_config.py    # Configuração de logging

tests/
├── test_data_processing.py  # Testes de processamento
└── test_embeddings.py       # Testes de embeddings

00_run_pipeline_job.py  # Notebook de orquestração
```

## 🔧 Configuração

### Parâmetros do Pipeline
```python
# Widgets configuráveis
tabela_magalu = "silver.embeddings_magalu_completo"
tabela_bemol = "silver.embeddings_bemol"
caminho_excel = "benchmarking_produtos.xlsx"
nome_tempview = "tempview_benchmarking_pares"
```

### Configurações de Similaridade
```python
SIMILARITY_HIGH = 0.95      # Muito similar
SIMILARITY_MODERATE = 0.85  # Moderadamente similar
SIMILARITY_LOW = 0.5        # Pouco similar
```

## 📋 Como Usar

### 1. Execução via Notebook
```python
# No notebook 00_run_pipeline_job.py
from src.main import executar_pipeline_completo

resultados = executar_pipeline_completo(
    tabela_magalu="silver.embeddings_magalu_completo",
    tabela_bemol="silver.embeddings_bemol",
    caminho_excel="benchmarking_produtos.xlsx"
)
```

### 2. Execução Programática
```python
from src.main import executar_pipeline_benchmarking

df_final, caminho_excel, nome_tempview = executar_pipeline_benchmarking(
    tabela_magalu="silver.embeddings_magalu_completo",
    tabela_bemol="silver.embeddings_bemol"
)
```

### 3. Consultas SQL
```sql
-- Produtos muito similares
SELECT title, marketplace, price, url, exclusividade, nivel_similaridade
FROM tempview_benchmarking_pares
WHERE nivel_similaridade = 'muito similar'
ORDER BY price DESC;

-- Produtos exclusivos
SELECT title, marketplace, price, url, exclusividade
FROM tempview_benchmarking_pares
WHERE exclusividade = 'sim'
ORDER BY price DESC;
```

## 🧪 Testes

### Executar Testes
```bash
# Testes de processamento
pytest tests/test_data_processing.py -v

# Testes de embeddings
pytest tests/test_embeddings.py -v

# Todos os testes
pytest tests/ -v
```

### Cobertura de Testes
- ✅ Limpeza de preços
- ✅ Classificação de similaridade
- ✅ Cálculo de diferença percentual
- ✅ Construção de URLs
- ✅ Preparação de DataFrames
- ✅ Cálculo de similaridade
- ✅ Criação de pares de produtos
- ✅ Processamento completo

## 📊 Saídas do Pipeline

### 1. DataFrame Final
```python
{
    "title": "Smartphone Samsung Galaxy",
    "marketplace": "Magalu",
    "price": 1299.0,
    "url": "https://www.magazineluiza.com.br/smartphone-samsung",
    "exclusividade": "não",
    "similaridade": 0.92,
    "nivel_similaridade": "muito similar",
    "diferenca_percentual": 8.5
}
```

### 2. Estatísticas
```python
{
    "total_produtos": 1500,
    "produtos_magalu": 750,
    "produtos_bemol": 750,
    "produtos_pareados": 1200,
    "produtos_exclusivos": 300,
    "muito_similar": 400,
    "moderadamente_similar": 600,
    "pouco_similar": 200,
    "exclusivo": 300
}
```

### 3. Arquivos Gerados
- **Excel**: `benchmarking_produtos.xlsx` (apenas colunas visíveis)
- **TempView**: `tempview_benchmarking_pares` (dados completos)

## 🔍 Monitoramento

### Logs
```python
# Exemplo de logs gerados
2024-01-15 10:30:00 - src.main - INFO - Iniciando pipeline de benchmarking
2024-01-15 10:30:05 - src.data_processing - INFO - DataFrame Magalu preparado: 750 produtos
2024-01-15 10:30:10 - src.embeddings - INFO - Similaridade calculada: 750 produtos Magalu vs 750 produtos Bemol
2024-01-15 10:30:15 - src.reporting - INFO - Relatório Excel exportado: benchmarking_produtos.xlsx
```

### Métricas de Performance
- Tempo de processamento
- Número de produtos processados
- Taxa de matching
- Distribuição de similaridade

## 🛠️ Manutenção

### Adicionar Novo Marketplace
1. Adicionar URL em `MARKETPLACE_URLS`
2. Implementar função de preparação específica
3. Atualizar testes
4. Documentar mudanças

### Ajustar Thresholds
```python
# Em src/config.py
SIMILARITY_HIGH = 0.90      # Ajustar conforme necessidade
SIMILARITY_MODERATE = 0.80  # Ajustar conforme necessidade
```

### Otimizar Performance
```python
# Em src/config.py
BATCH_SIZE = 500            # Aumentar para mais produtos
MAX_PRODUCTS_PER_BATCH = 2000  # Aumentar limite
```

## 📈 Próximos Passos

### Melhorias Planejadas
- [ ] Dashboard interativo
- [ ] Alertas automáticos
- [ ] Análise temporal de preços
- [ ] Integração com mais marketplaces
- [ ] Machine Learning para otimização

### Monitoramento Avançado
- [ ] Métricas de negócio
- [ ] Alertas de preços
- [ ] Análise de tendências
- [ ] Relatórios automáticos

## 🤝 Contribuição

### Padrões de Código
- Seguir PEP 8
- Usar type hints
- Documentar funções
- Criar testes para novas funcionalidades

### Estrutura de Commits
```
feat: adicionar nova funcionalidade
fix: corrigir bug
docs: atualizar documentação
test: adicionar testes
refactor: refatorar código
```

## 📞 Suporte

Para dúvidas ou problemas:
- Verificar logs em `/tmp/web_scraping.log`
- Executar testes para validar funcionalidades
- Consultar documentação inline do código
- Revisar configurações em `src/config.py` 