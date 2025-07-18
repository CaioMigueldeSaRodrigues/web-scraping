import pytest
import pandas as pd
import numpy as np
from src.data_processing import (
    limpar_preco,
    classificar_similaridade,
    percentual_diferenca,
    preparar_dataframe_embeddings,
    construir_url_completa,
    identificar_produtos_exclusivos,
    calcular_diferenca_precos_pares,
    definir_colunas_relatorio,
    remover_duplicados_por_marketplace,
    limpar_e_preparar_dataframe_final
)


class TestLimparPreco:
    """Testes para função de limpeza de preços"""
    
    def test_preco_formato_brasileiro(self):
        """Testa preços em formato brasileiro (6.599,00)"""
        assert limpar_preco("R$ 6.599,00") == 6599.0
        assert limpar_preco("1.234,56") == 1234.56
        assert limpar_preco("999,99") == 999.99
    
    def test_preco_formato_simples(self):
        """Testa preços em formato simples"""
        assert limpar_preco("123.45") == 123.45
        assert limpar_preco("100") == 100.0
        assert limpar_preco("0.99") == 0.99
    
    def test_preco_com_palavras(self):
        """Testa preços com palavras extras"""
        assert limpar_preco("R$ 100,00 ou 3x de R$ 33,33") == 100.0
        assert limpar_preco("Preço: 50,00") == 50.0
    
    def test_preco_invalido(self):
        """Testa preços inválidos"""
        assert limpar_preco("") == 0.0
        assert limpar_preco("abc") == 0.0
        assert limpar_preco(None) == 0.0


class TestClassificarSimilaridade:
    """Testes para função de classificação de similaridade"""
    
    def test_exclusivo(self):
        """Testa classificação de produto exclusivo"""
        assert classificar_similaridade(-1) == "exclusivo"
    
    def test_muito_similar(self):
        """Testa classificação de muito similar"""
        assert classificar_similaridade(0.95) == "muito similar"
        assert classificar_similaridade(0.90) == "muito similar"
    
    def test_moderadamente_similar(self):
        """Testa classificação de moderadamente similar"""
        assert classificar_similaridade(0.85) == "moderadamente similar"
        assert classificar_similaridade(0.70) == "moderadamente similar"
        assert classificar_similaridade(0.50) == "moderadamente similar"
    
    def test_pouco_similar(self):
        """Testa classificação de pouco similar"""
        assert classificar_similaridade(0.49) == "pouco similar"
        assert classificar_similaridade(0.10) == "pouco similar"
        assert classificar_similaridade(0.0) == "pouco similar"


class TestPercentualDiferenca:
    """Testes para função de cálculo de diferença percentual"""
    
    def test_diferenca_normal(self):
        """Testa cálculo de diferença percentual normal"""
        assert percentual_diferenca(100, 90) == 10.526315789473685
        assert percentual_diferenca(50, 100) == 66.66666666666666
    
    def test_precos_iguais(self):
        """Testa quando preços são guais"""
        assert percentual_diferenca(100, 100) == 0.0
    
    def test_preco_zero(self):
        """Testa quando um dos preços é zero"""
        assert percentual_diferenca(0, 100) is None
        assert percentual_diferenca(100, 0) is None
        assert percentual_diferenca(0, 0) is None


class TestConstruirUrlCompleta:
    """Testes para função de construção de URL"""
    
    def test_url_relativa(self):
        """Testa URL relativa"""
        assert construir_url_completa("/produto/123", "https://www.magazineluiza.com.br") == "https://www.magazineluiza.com.br/produto/123"
    
    def test_url_absoluta(self):
        """Testa URL absoluta"""
        url_absoluta = "https://www.magazineluiza.com.br/produto/123"
        assert construir_url_completa(url_absoluta, "https://www.magazineluiza.com.br") == url_absoluta
    
    def test_url_vazia(self):
        """Testa URL vazia"""
        assert construir_url_completa("", "https://www.magazineluiza.com.br") == "https://www.magazineluiza.com.br"


class TestDefinirColunasRelatorio:
    """Testes para função de definição de colunas do relatório"""
    
    def test_colunas_padrao(self):
        """Testa definição de colunas padrão"""
        df = pd.DataFrame({
            "title": ["produto1"],
            "price": [100],
            "url": ["http://test.com"],
            "similaridade": [0.8],
            "nivel_similaridade": ["muito similar"],
            "diferenca_percentual": [10.5]
        })
        
        colunas_visiveis, colunas_ocultas = definir_colunas_relatorio(df)
        
        assert "title" in colunas_visiveis
        assert "price" in colunas_visiveis
        assert "url" in colunas_visiveis
        assert "similaridade" in colunas_ocultas
        assert "nivel_similaridade" in colunas_ocultas
        assert "diferenca_percentual" in colunas_ocultas


class TestPrepararDataframeEmbeddings:
    """Testes para função de preparação de DataFrame com embeddings"""
    
    def test_preparacao_normal(self):
        """Testa preparação normal de DataFrame"""
        df = pd.DataFrame({
            "title": ["produto1", "produto2"],
            "price": ["R$ 100,00", "R$ 200,00"],
            "embedding": [[1, 2, 3], [4, 5, 6]]
        })
        
        df_prep = preparar_dataframe_embeddings(df, "Magalu")
        
        assert len(df_prep) == 2
        assert "marketplace" in df_prep.columns
        assert df_prep["marketplace"].iloc[0] == "Magalu"
        assert df_prep["price"].iloc[0] == 100.0
        assert isinstance(df_prep["embedding"].iloc[0], np.ndarray)
    
    def test_embedding_nulo(self):
        """Testa DataFrame com embedding nulo"""
        df = pd.DataFrame({
            "title": ["produto1", "produto2"],
            "price": ["R$ 100,00", "R$ 200,00"],
            "embedding": [[1, 2, 3], None]
        })
        
        df_prep = preparar_dataframe_embeddings(df, "Bemol")
        
        assert len(df_prep) == 1  # Remove linha com embedding nulo
        assert df_prep["marketplace"].iloc[0] == "Bemol"


class TestCalcularDiferencaPrecosPares:
    """Testes para função de cálculo de diferença de preços entre pares"""
    
    def test_calculo_pares(self):
        """Testa cálculo de diferença para pares"""
        df = pd.DataFrame({
            "price": [100, 110, 200, 180],
            "similaridade": [0.9, 0.9, 0.8, 0.8]
        })
        
        df_result = calcular_diferenca_precos_pares(df)
        
        # Verifica se a coluna foi adicionada
        assert "diferenca_percentual" in df_result.columns
        
        # Verifica se os valores foram calculados para pares
        # Primeiro par: 100 e 110 -> diferença de ~9.52%
        # Segundo par: 200 e 180 -> diferença de ~10.53%
        assert df_result["diferenca_percentual"].iloc[0] is not None
        assert df_result["diferenca_percentual"].iloc[2] is not None


class TestRemoverDuplicadosPorMarketplace:
    """Testes para função de remoção de duplicados por marketplace"""
    
    def test_remocao_duplicados_basica(self):
        """Testa remoção básica de duplicados"""
        df = pd.DataFrame({
            "title": ["produto1", "produto1", "produto2", "produto3"],
            "marketplace": ["Magalu", "Magalu", "Bemol", "Magalu"],
            "price": [100, 90, 150, 200]  # produto1 Magalu tem duas versões
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter apenas 3 produtos (produto1 mais barato, produto2, produto3)
        assert len(df_limpo) == 3
        
        # Deve manter o produto1 mais barato (90)
        produto1_magalu = df_limpo[df_limpo["title"] == "produto1"]
        assert len(produto1_magalu) == 1
        assert produto1_magalu["price"].iloc[0] == 90
    
    def test_remocao_duplicados_diferentes_marketplaces(self):
        """Testa que produtos iguais em marketplaces diferentes não são removidos"""
        df = pd.DataFrame({
            "title": ["produto1", "produto1", "produto2"],
            "marketplace": ["Magalu", "Bemol", "Magalu"],
            "price": [100, 150, 200]
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter todos os 3 produtos (produto1 em ambos marketplaces)
        assert len(df_limpo) == 3
    
    def test_remocao_duplicados_sem_duplicados(self):
        """Testa DataFrame sem duplicados"""
        df = pd.DataFrame({
            "title": ["produto1", "produto2", "produto3"],
            "marketplace": ["Magalu", "Bemol", "Magalu"],
            "price": [100, 150, 200]
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter todos os produtos
        assert len(df_limpo) == 3
    
    def test_remocao_duplicados_mesmo_titulo_diferentes_precos(self):
        """Testa remoção quando mesmo título tem preços diferentes no mesmo marketplace"""
        df = pd.DataFrame({
            "title": ["Smartphone Galaxy", "Smartphone Galaxy", "Smartphone Galaxy"],
            "marketplace": ["Magalu", "Magalu", "Bemol"],
            "price": [1200, 1100, 1300]  # Magalu tem duas versões do mesmo produto
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter apenas 2 produtos (mais barato do Magalu + Bemol)
        assert len(df_limpo) == 2
        
        # Verifica que manteve o mais barato do Magalu
        magalu_produtos = df_limpo[df_limpo["marketplace"] == "Magalu"]
        assert len(magalu_produtos) == 1
        assert magalu_produtos["price"].iloc[0] == 1100
    
    def test_remocao_duplicados_multiplos_marketplaces(self):
        """Testa remoção com múltiplos duplicados em diferentes marketplaces"""
        df = pd.DataFrame({
            "title": ["Produto A", "Produto A", "Produto A", "Produto B", "Produto B"],
            "marketplace": ["Magalu", "Magalu", "Bemol", "Magalu", "Bemol"],
            "price": [100, 80, 120, 200, 180]  # Produto A tem duplicado no Magalu
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter 4 produtos (Produto A mais barato do Magalu + Bemol, Produto B de ambos)
        assert len(df_limpo) == 4
        
        # Verifica que manteve o mais barato do Magalu para Produto A
        produto_a_magalu = df_limpo[(df_limpo["title"] == "Produto A") & (df_limpo["marketplace"] == "Magalu")]
        assert len(produto_a_magalu) == 1
        assert produto_a_magalu["price"].iloc[0] == 80
    
    def test_remocao_duplicados_ordenacao_preco(self):
        """Testa que a ordenação por preço funciona corretamente"""
        df = pd.DataFrame({
            "title": ["Produto Teste", "Produto Teste", "Produto Teste"],
            "marketplace": ["Magalu", "Magalu", "Magalu"],
            "price": [500, 300, 400]  # Ordem aleatória, deve manter o mais barato (300)
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter apenas 1 produto (o mais barato)
        assert len(df_limpo) == 1
        assert df_limpo["price"].iloc[0] == 300
    
    def test_remocao_duplicados_precos_iguais(self):
        """Testa comportamento quando preços são iguais"""
        df = pd.DataFrame({
            "title": ["Produto Igual", "Produto Igual"],
            "marketplace": ["Magalu", "Magalu"],
            "price": [100, 100]  # Preços iguais
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter apenas 1 produto (primeiro encontrado)
        assert len(df_limpo) == 1
        assert df_limpo["price"].iloc[0] == 100
    
    def test_remocao_duplicados_dataframe_vazio(self):
        """Testa comportamento com DataFrame vazio"""
        df = pd.DataFrame(columns=["title", "marketplace", "price"])
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve retornar DataFrame vazio
        assert len(df_limpo) == 0
        assert list(df_limpo.columns) == ["title", "marketplace", "price"]
    
    def test_remocao_duplicados_um_produto(self):
        """Testa comportamento com apenas um produto"""
        df = pd.DataFrame({
            "title": ["Produto Único"],
            "marketplace": ["Magalu"],
            "price": [100]
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter o produto único
        assert len(df_limpo) == 1
        assert df_limpo["title"].iloc[0] == "Produto Único"
        assert df_limpo["price"].iloc[0] == 100
    
    def test_remocao_duplicados_100_percent_similaridade_mesmo_marketplace(self):
        """Testa remoção específica para produtos com 100% similaridade no mesmo marketplace"""
        df = pd.DataFrame({
            "title": ["Produto Identico", "Produto Identico", "Produto Diferente"],
            "marketplace": ["Magalu", "Magalu", "Bemol"],
            "price": [100, 80, 150],
            "similaridade": [1.0, 1.0, 0.8]  # 100% similaridade para duplicados no Magalu
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter apenas 2 produtos (mais barato do Magalu + Bemol)
        assert len(df_limpo) == 2
        
        # Verifica que manteve o mais barato do Magalu
        magalu_produtos = df_limpo[df_limpo["marketplace"] == "Magalu"]
        assert len(magalu_produtos) == 1
        assert magalu_produtos["price"].iloc[0] == 80
        assert magalu_produtos["similaridade"].iloc[0] == 1.0
    
    def test_remocao_duplicados_100_percent_similaridade_diferentes_marketplaces(self):
        """Testa que produtos com 100% similaridade em marketplaces diferentes NÃO são removidos"""
        df = pd.DataFrame({
            "title": ["Produto Identico", "Produto Identico", "Produto Outro"],
            "marketplace": ["Magalu", "Bemol", "Magalu"],
            "price": [100, 120, 200],
            "similaridade": [1.0, 1.0, 0.8]  # 100% similaridade entre Magalu e Bemol
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter todos os 3 produtos (não são duplicados, são de marketplaces diferentes)
        assert len(df_limpo) == 3
        
        # Verifica que manteve produtos de ambos marketplaces
        assert len(df_limpo[df_limpo["marketplace"] == "Magalu"]) == 2
        assert len(df_limpo[df_limpo["marketplace"] == "Bemol"]) == 1
    
    def test_remocao_duplicados_99_percent_similaridade_nao_remove(self):
        """Testa que produtos com 99% similaridade NÃO são considerados duplicados"""
        df = pd.DataFrame({
            "title": ["Produto Similar", "Produto Similar", "Produto Outro"],
            "marketplace": ["Magalu", "Magalu", "Bemol"],
            "price": [100, 80, 150],
            "similaridade": [0.99, 0.99, 0.8]  # 99% similaridade (não é 100%)
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter todos os 3 produtos (99% não é considerado duplicado)
        assert len(df_limpo) == 3
        
        # Verifica que manteve ambos produtos do Magalu
        magalu_produtos = df_limpo[df_limpo["marketplace"] == "Magalu"]
        assert len(magalu_produtos) == 2
    
    def test_remocao_duplicados_mistura_similaridades(self):
        """Testa cenário com mistura de similaridades diferentes"""
        df = pd.DataFrame({
            "title": ["Produto A", "Produto A", "Produto B", "Produto B", "Produto C"],
            "marketplace": ["Magalu", "Magalu", "Magalu", "Bemol", "Magalu"],
            "price": [100, 80, 200, 180, 300],
            "similaridade": [1.0, 1.0, 0.99, 0.99, 0.8]  # A=100%, B=99%, C=80%
        })
        
        df_limpo = remover_duplicados_por_marketplace(df)
        
        # Deve manter 4 produtos:
        # - Produto A: 1 (mais barato do Magalu, 100% similaridade)
        # - Produto B: 2 (ambos mantidos, 99% não é considerado duplicado)
        # - Produto C: 1 (único)
        assert len(df_limpo) == 4
        
        # Verifica que manteve o mais barato do Magalu para Produto A
        produto_a_magalu = df_limpo[(df_limpo["title"] == "Produto A") & (df_limpo["marketplace"] == "Magalu")]
        assert len(produto_a_magalu) == 1
        assert produto_a_magalu["price"].iloc[0] == 80
        
        # Verifica que manteve ambos Produto B (99% não é duplicado)
        produto_b = df_limpo[df_limpo["title"] == "Produto B"]
        assert len(produto_b) == 2


class TestLimparEPrepararDataframeFinal:
    """Testes para função de limpeza completa do DataFrame final"""
    
    def test_limpeza_completa(self):
        """Testa limpeza completa do DataFrame"""
        df = pd.DataFrame({
            "title": ["produto1", "produto1", "produto2", "produto3"],
            "marketplace": ["Magalu", "Magalu", "Bemol", "Magalu"],
            "price": [100, 90, 150, 200],
            "exclusividade": ["não", "não", "não", "sim"],
            "similaridade": [0.8, 0.8, 0.9, -1]
        })
        
        df_limpo = limpar_e_preparar_dataframe_final(df)
        
        # Verifica se duplicados foram removidos
        assert len(df_limpo) == 3
        
        # Verifica se colunas foram adicionadas
        assert "nivel_similaridade" in df_limpo.columns
        assert "diferenca_percentual" in df_limpo.columns
        
        # Verifica se ordenação está correta (exclusivos primeiro)
        assert df_limpo["exclusividade"].iloc[0] == "sim"
    
    def test_limpeza_dataframe_vazio(self):
        """Testa limpeza de DataFrame vazio"""
        df = pd.DataFrame()
        
        df_limpo = limpar_e_preparar_dataframe_final(df)
        
        # Deve retornar DataFrame vazio mas com estrutura correta
        assert len(df_limpo) == 0
        assert "nivel_similaridade" in df_limpo.columns
        assert "diferenca_percentual" in df_limpo.columns
    
    def test_limpeza_com_duplicados_100_percent_similaridade(self):
        """Testa limpeza com duplicados de 100% similaridade no mesmo marketplace"""
        df = pd.DataFrame({
            "title": ["Produto Identico", "Produto Identico", "Produto Diferente"],
            "marketplace": ["Magalu", "Magalu", "Bemol"],
            "price": [100, 80, 150],  # Produto Identico tem duas versões no Magalu
            "exclusividade": ["não", "não", "não"],
            "similaridade": [1.0, 1.0, 0.8]  # 100% similaridade para duplicados
        })
        
        df_limpo = limpar_e_preparar_dataframe_final(df)
        
        # Deve manter apenas 2 produtos (mais barato do Magalu + Bemol)
        assert len(df_limpo) == 2
        
        # Verifica que manteve o mais barato do Magalu
        magalu_produtos = df_limpo[df_limpo["marketplace"] == "Magalu"]
        assert len(magalu_produtos) == 1
        assert magalu_produtos["price"].iloc[0] == 80
        assert magalu_produtos["similaridade"].iloc[0] == 1.0
    
    def test_limpeza_sem_duplicados_100_percent_similaridade(self):
        """Testa limpeza sem duplicados mas com 100% similaridade entre marketplaces"""
        df = pd.DataFrame({
            "title": ["Produto Identico", "Produto Identico", "Produto Outro"],
            "marketplace": ["Magalu", "Bemol", "Magalu"],
            "price": [100, 120, 200],
            "exclusividade": ["não", "não", "sim"],
            "similaridade": [1.0, 1.0, -1]  # 100% similaridade entre Magalu e Bemol
        })
        
        df_limpo = limpar_e_preparar_dataframe_final(df)
        
        # Deve manter todos os 3 produtos (não são duplicados, são de marketplaces diferentes)
        assert len(df_limpo) == 3
        
        # Verifica que manteve produtos de ambos marketplaces
        assert len(df_limpo[df_limpo["marketplace"] == "Magalu"]) == 2
        assert len(df_limpo[df_limpo["marketplace"] == "Bemol"]) == 1


if __name__ == "__main__":
    pytest.main([__file__]) 