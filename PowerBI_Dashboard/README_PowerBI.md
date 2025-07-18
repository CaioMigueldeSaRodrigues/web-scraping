# 📊 Dashboard Power BI - Benchmarking Bemol vs Magalu

## 🎯 Visão Geral
Este dashboard analisa a competitividade da Bemol em relação ao Magazine Luiza, fornecendo insights sobre preços, produtos exclusivos e oportunidades de mercado.

---

## 📁 Estrutura de Dados

### **Tabela Principal: `benchmarking_produtos`**
Fonte de dados principal para análises de competitividade.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `title` | Text | Nome do produto |
| `marketplace` | Text | "Bemol" ou "Magalu" |
| `price` | Decimal | Preço do produto |
| `url` | Text | Link do produto |
| `exclusividade` | Text | "sim" (exclusivo) ou "não" (pareado) |
| `similaridade` | Decimal | Score de similaridade (0-1 ou -1 para exclusivo) |
| `nivel_similaridade` | Text | "exclusivo", "muito similar", "moderadamente similar", "pouco similar" |
| `diferenca_percentual` | Decimal | Diferença percentual entre pares |

---

## 🔧 Medidas DAX

### **📈 KPIs Principais**

```dax
// Total de Produtos
Total Produtos = COUNTROWS('benchmarking_produtos')

// Produtos Bemol
Total Bemol = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[marketplace] = "Bemol"
)

// Produtos Magalu
Total Magalu = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[marketplace] = "Magalu"
)

// Produtos Exclusivos Bemol
Exclusivos Bemol = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[marketplace] = "Bemol",
    'benchmarking_produtos'[exclusividade] = "sim"
)

// Produtos Exclusivos Magalu
Exclusivos Magalu = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[marketplace] = "Magalu",
    'benchmarking_produtos'[exclusividade] = "sim"
)

// Produtos Pareados
Produtos Pareados = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[exclusividade] = "não"
) / 2
```

### **💰 Análise de Preços**

```dax
// Preço Médio Bemol
Preço Médio Bemol = CALCULATE(
    AVERAGE('benchmarking_produtos'[price]),
    'benchmarking_produtos'[marketplace] = "Bemol"
)

// Preço Médio Magalu
Preço Médio Magalu = CALCULATE(
    AVERAGE('benchmarking_produtos'[price]),
    'benchmarking_produtos'[marketplace] = "Magalu"
)

// Diferença Percentual Média (apenas produtos pareados)
Diferença Percentual Média = CALCULATE(
    AVERAGE('benchmarking_produtos'[diferenca_percentual]),
    'benchmarking_produtos'[exclusividade] = "não",
    NOT(ISBLANK('benchmarking_produtos'[diferenca_percentual]))
)

// Produtos onde Bemol é mais barata
Bemol Mais Barata = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[marketplace] = "Bemol",
    'benchmarking_produtos'[exclusividade] = "não",
    'benchmarking_produtos'[price] < RELATED('benchmarking_produtos'[price])
)

// Produtos onde Magalu é mais barata
Magalu Mais Barata = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[marketplace] = "Bemol",
    'benchmarking_produtos'[exclusividade] = "não",
    'benchmarking_produtos'[price] > RELATED('benchmarking_produtos'[price])
)
```

### **🎯 Análise de Similaridade**

```dax
// Produtos Muito Similares
Muito Similares = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[nivel_similaridade] = "muito similar"
)

// Produtos Moderadamente Similares
Moderadamente Similares = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[nivel_similaridade] = "moderadamente similar"
)

// Produtos Pouco Similares
Pouco Similares = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[nivel_similaridade] = "pouco similar"
)

// Produtos Exclusivos
Produtos Exclusivos = CALCULATE(
    COUNTROWS('benchmarking_produtos'),
    'benchmarking_produtos'[nivel_similaridade] = "exclusivo"
)
```

### **📊 Medidas de Competitividade**

```dax
// Percentual de Produtos onde Bemol é mais barata
% Bemol Mais Barata = DIVIDE(
    [Bemol Mais Barata],
    [Produtos Pareados],
    0
)

// Percentual de Produtos onde Magalu é mais barata
% Magalu Mais Barata = DIVIDE(
    [Magalu Mais Barata],
    [Produtos Pareados],
    0
)

// Vantagem Competitiva Bemol
Vantagem Competitiva Bemol = [% Bemol Mais Barata] - [% Magalu Mais Barata]

// Diferença de Preço Média (Bemol vs Magalu)
Diferença Preço Média = 
VAR PrecoBemol = [Preço Médio Bemol]
VAR PrecoMagalu = [Preço Médio Magalu]
RETURN
    DIVIDE(PrecoMagalu - PrecoBemol, PrecoBemol, 0)
```

---

## 🔗 Relações de Dados

### **Tabela Principal (Auto-relacionada)**
```dax
// Relação para análise de pares
'benchmarking_produtos'[title] -> 'benchmarking_produtos'[title]
```

### **Tabelas de Dimensão (Criar se necessário)**

```dax
// Tabela de Marketplace
Marketplace = DISTINCT('benchmarking_produtos'[marketplace])

// Tabela de Níveis de Similaridade
Niveis Similaridade = DISTINCT('benchmarking_produtos'[nivel_similaridade])

// Tabela de Exclusividade
Exclusividade = DISTINCT('benchmarking_produtos'[exclusividade])
```

---

## 📈 Visualizações Recomendadas

### **1. KPIs Principais (Cartões)**
- Total de Produtos
- Produtos Pareados
- Exclusivos Bemol
- Exclusivos Magalu
- Diferença Percentual Média

### **2. Análise de Preços**
- **Gráfico de Barras:** Top 10 produtos com maior diferença percentual
- **Gráfico de Barras:** Top 10 produtos com menor diferença percentual
- **Gráfico de Pizza:** Distribuição de competitividade (Bemol mais barata vs Magalu mais barata)

### **3. Análise de Similaridade**
- **Gráfico de Barras:** Distribuição por nível de similaridade
- **Gráfico de Dispersão:** Similaridade vs Diferença percentual

### **4. Produtos Exclusivos**
- **Tabela:** Lista de produtos exclusivos do Magalu (oportunidades)
- **Tabela:** Lista de produtos exclusivos da Bemol (vantagem competitiva)

### **5. Análise Temporal (se houver dados históricos)**
- **Gráfico de Linha:** Evolução da diferença percentual média
- **Gráfico de Linha:** Evolução do número de produtos pareados

---

## 🔧 Código M (Power Query)

### **Limpeza de Dados**
```m
let
    Source = Excel.Workbook(File.Contents("benchmarking_produtos.xlsx"), null, true),
    benchmarking_produtos_Sheet = Source{[Item="benchmarking_produtos",Kind="Sheet"]}[Data],
    
    // Remover linhas vazias
    RemoveEmptyRows = Table.SelectRows(benchmarking_produtos_Sheet, each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
    
    // Converter tipos de dados
    ChangeTypes = Table.TransformColumnTypes(RemoveEmptyRows,{
        {"title", type text},
        {"marketplace", type text},
        {"price", type number},
        {"url", type text},
        {"exclusividade", type text},
        {"similaridade", type number},
        {"nivel_similaridade", type text},
        {"diferenca_percentual", type number}
    }),
    
    // Adicionar coluna de categoria (extrair do título)
    AddCategory = Table.AddColumn(ChangeTypes, "categoria", each 
        if Text.Contains([title], "Geladeira") then "Eletrodomésticos"
        else if Text.Contains([title], "Fogão") then "Eletrodomésticos"
        else if Text.Contains([title], "Máquina") then "Eletrodomésticos"
        else if Text.Contains([title], "Smartphone") then "Eletrônicos"
        else if Text.Contains([title], "Notebook") then "Eletrônicos"
        else if Text.Contains([title], "TV") then "Eletrônicos"
        else "Outros"
    ),
    
    // Adicionar coluna de faixa de preço
    AddPriceRange = Table.AddColumn(AddCategory, "faixa_preco", each 
        if [price] <= 100 then "Até R$ 100"
        else if [price] <= 500 then "R$ 100 - R$ 500"
        else if [price] <= 1000 then "R$ 500 - R$ 1.000"
        else if [price] <= 5000 then "R$ 1.000 - R$ 5.000"
        else "Acima de R$ 5.000"
    )
in
    AddPriceRange
```

### **Transformação para Análise de Pares**
```m
let
    Source = benchmarking_produtos,
    
    // Filtrar apenas produtos pareados
    ParesOnly = Table.SelectRows(Source, each [exclusividade] = "não"),
    
    // Agrupar por título para criar pares
    GroupByTitle = Table.Group(ParesOnly, {"title"}, {
        {"Bemol_Price", each Table.SelectRows(_, each [marketplace] = "Bemol"){0}[price]},
        {"Magalu_Price", each Table.SelectRows(_, each [marketplace] = "Magalu"){0}[price]},
        {"Bemol_URL", each Table.SelectRows(_, each [marketplace] = "Bemol"){0}[url]},
        {"Magalu_URL", each Table.SelectRows(_, each [marketplace] = "Magalu"){0}[url]},
        {"Similaridade", each Table.SelectRows(_, each [marketplace] = "Bemol"){0}[similaridade]},
        {"Nivel_Similaridade", each Table.SelectRows(_, each [marketplace] = "Bemol"){0}[nivel_similaridade]},
        {"Diferenca_Percentual", each Table.SelectRows(_, each [marketplace] = "Bemol"){0}[diferenca_percentual]}
    }),
    
    // Calcular vantagem competitiva
    AddCompetitiveAdvantage = Table.AddColumn(GroupByTitle, "vantagem_competitiva", each 
        if [Bemol_Price] < [Magalu_Price] then "Bemol Mais Barata"
        else if [Bemol_Price] > [Magalu_Price] then "Magalu Mais Barata"
        else "Preços Iguais"
    )
in
    AddCompetitiveAdvantage
```

---

## 📋 Checklist de Implementação

### **✅ Configuração Inicial**
- [ ] Importar dados do Excel/CSV
- [ ] Aplicar transformações M
- [ ] Configurar relações entre tabelas
- [ ] Criar medidas DAX básicas

### **✅ Visualizações**
- [ ] KPIs principais (cartões)
- [ ] Gráficos de análise de preços
- [ ] Tabelas de produtos exclusivos
- [ ] Gráficos de similaridade

### **✅ Interatividade**
- [ ] Filtros por marketplace
- [ ] Filtros por nível de similaridade
- [ ] Filtros por faixa de preço
- [ ] Drill-down por categoria

### **✅ Formatação**
- [ ] Tema corporativo (cores Bemol)
- [ ] Formatação de números (moeda brasileira)
- [ ] Formatação de percentuais
- [ ] Tooltips informativos

---

## 🎨 Tema de Cores Sugerido

```dax
// Cores corporativas Bemol
Bemol Azul = "#0B5394"
Bemol Laranja = "#FF6B35"
Magalu Verde = "#00A650"
Neutro Cinza = "#6C757D"
```

---

## 📊 Métricas de Sucesso

### **KPIs de Negócio**
- **Competitividade:** % de produtos onde Bemol é mais barata
- **Cobertura:** % de produtos do Magalu que temos equivalentes
- **Oportunidades:** Número de produtos exclusivos do Magalu
- **Vantagem:** Diferença percentual média favorável

### **Métricas Técnicas**
- **Atualização:** Frequência de atualização dos dados
- **Qualidade:** % de produtos com similaridade > 0.85
- **Performance:** Tempo de carregamento do dashboard
- **Usabilidade:** Número de usuários ativos

---

## 🔄 Processo de Atualização

1. **Executar pipeline** no Databricks
2. **Exportar dados** para Excel/CSV
3. **Atualizar fonte** no Power BI
4. **Validar medidas** e visualizações
5. **Publicar** para usuários finais

---

## 📞 Suporte

Para dúvidas sobre implementação ou personalização do dashboard, consulte a documentação do projeto principal ou entre em contato com a equipe de dados. 