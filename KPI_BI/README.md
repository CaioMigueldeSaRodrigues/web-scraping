# Análise de Concorrência - Documentação de Dados

## Visão Geral
Este projeto automatiza a comparação de preços e catálogo entre a Bemol (VTEX) e o concorrente Magazine Luiza. O pipeline realiza scraping, processamento de similaridade de produtos via NLP e gera dois produtos de dados distintos para diferentes públicos.

---

## Produtos de Dados Gerados

### Produto 1: Tabela Analítica (Formato Largo)
Esta é a fonte de verdade para análises internas, BI e Data Science. Cada linha representa um par de produtos comparados.

- **Propósito:** Análise de dados, dashboards em Power BI/Tableau, estudos de precificação.
- **Formato de Entrega:** Tabela Delta (ex: `silver.analise_concorrencia_magalu`).
- **Schema da Tabela:**
  | Coluna | Tipo de Dado | Descrição |
  | :--- | :--- | :--- |
  | `produto_site` | `string` | Título do produto no site concorrente (Magalu). |
  | `produto_tabela` | `string` | Título do produto correspondente na nossa base (Bemol/VTEX). |
  | `similaridade` | `double` | Score de similaridade de cosseno (0 a 1) entre os títulos. |
  | `preco_site` | `double` | Preço do produto no concorrente. |
  | `preco_tabela` | `double` | Nosso preço para o produto. |
  | `diferencial_percentual` | `double` | Variação percentual de preço: `((preco_site - preco_tabela) / preco_tabela)`. Negativo significa que estamos mais baratos. |
  | `url_site` | `string` | Link para o produto no concorrente. |
  | `url_tabela` | `string` | Link para o nosso produto. |
  | `categoria_site` | `string` | Categoria do produto extraída do concorrente. |
  | `id_tabela` | `string` | ID do nosso produto na base de dados. |
  | `exclusivo` | `boolean` | `True` se o produto só existe no site concorrente. |

### Produto 2: Relatório de Negócios (Formato Longo)
Este é um relatório formatado para fácil consumo pelas equipes de Comercial e Marketing, entregue via email. O formato é otimizado para visualização e não para análise de dados.

- **Propósito:** Tomada de decisão rápida, identificação de oportunidades, análise de catálogo.
- **Formato de Entrega:** Planilha Excel (`.xlsx`) em anexo de email.
- **Schema da Planilha:**
  | Coluna | Descrição |
  | :--- | :--- |
  | `title` | Título mestre do produto (baseado no concorrente). |
  | `marketplace` | Indica se a linha se refere a 'Bemol' ou 'Magalu'. |
  | `price` | Preço do produto no respectivo marketplace. |
  | `url` | Link do produto no respectivo marketplace. |
  | `exclusividade` | "Sim" se o produto só existe no concorrente; "Não" caso contrário. |
  | `diferenca_percentual` | A diferença de preço, aplicada a ambas as linhas do par. |

---

## Estratégias de Análise e KPIs (Power BI, usando a Tabela Analítica)

**Medidas DAX Sugeridas:**
- **Contagem de Exclusivos:**
  `Exclusivos Concorrente = CALCULATE(COUNTROWS('TabelaAnalitica'), 'TabelaAnalitica'[exclusivo] = TRUE())`
- **Diferencial de Preço Médio:**
  `Avg Price Diff % = AVERAGE('TabelaAnalitica'[diferencial_percentual])`
- **Contagem de Produtos Mais Caros:**
  `Produtos Mais Caros (Nós) = CALCULATE(COUNTROWS('TabelaAnalitica'), 'TabelaAnalitica'[diferencial_percentual] < 0)`
- **Contagem de Produtos Mais Baratos:**
  `Produtos Mais Baratos (Nós) = CALCULATE(COUNTROWS('TabelaAnalitica'), 'TabelaAnalitica'[diferencial_percentual] > 0)`

**Visualizações Recomendadas:**
1. **Cartão (KPI):** `Exclusivos Concorrente`.
2. **Gráfico de Barras:** Top 10 Produtos com maior `diferencial_percentual` (onde estamos mais baratos).
3. **Gráfico de Barras:** Top 10 Produtos com menor `diferencial_percentual` (onde estamos mais caros).
4. **Tabela:** Lista de produtos exclusivos (`exclusivo = TRUE()`), com `produto_site`, `preco_site`, `categoria_site` e `url_site`, para análise de potencial de inclusão no catálogo. 