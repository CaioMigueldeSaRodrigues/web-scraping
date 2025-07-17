# KPI & BI - Documentação da Fonte de Dados

A fonte de dados para o BI é a tabela `relatorio_concorrencia.xlsx` gerada pelo pipeline.

### Colunas Relevantes

| Coluna | Descrição | Medida Power BI Sugerida (DAX) |
| :--- | :--- | :--- |
| `produto_site` | Nome do produto no concorrente. | |
| `similaridade` | Score de similaridade (0 a 1). | `Avg Similarity = AVERAGE('Relatorio'[similaridade])` |
| `preco_site` | Preço no concorrente. | |
| `preco_tabela` | Nosso preço. | |
| `diferencial_percentual` | Variação de preço. Negativo = mais baratos. | `Avg Price Difference % = AVERAGE('Relatorio'[diferencial_percentual])` |
| `categoria_site` | Categoria do produto no concorrente. | |
| `exclusivo` | (TRUE/FALSE) Se o produto só existe no concorrente. | `Count of Exclusives = CALCULATE(COUNTROWS('Relatorio'), 'Relatorio'[exclusivo] = TRUE())` |

### Arquitetura de Dashboard Sugerida

**Power BI / Tableau** conectado diretamente à tabela Delta final que pode ser gerada a partir do `final_report_df`. Isso permite atualização automática.

### Indicadores Chave

1.  **KPI: Nº de Exclusivos do Concorrente:** `COUNTROWS(FILTER('Relatorio', 'Relatorio'[exclusivo] = TRUE()))`
2.  **Gráfico de Barras: Top 10 Produtos Onde Estamos Mais Caros:** Filtrar `diferencial_percentual > 0` e ordenar.
3.  **Gráfico de Barras: Top 10 Produtos Onde Estamos Mais Baratos:** Filtrar `diferencial_percentual < 0` e ordenar.
4.  **Tabela: Lista de Exclusivos para Análise de Compra:** Filtrar `exclusivo = TRUE()` e ordenar por preço. 