import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from src.embeddings import (
    calcular_similaridade_embeddings,
    criar_pares_produtos,
    construir_url_completa,
    processar_embeddings_completos
)


class TestCalcularSimilaridadeEmbeddings:
    """Testes para função de cálculo de similaridade entre embeddings"""
    
    def test_calculo_similaridade(self):
        """Testa cálculo básico de similaridade"""
        # Cria DataFrames de teste
        df_magalu = pd.DataFrame({
            "title": ["produto1", "produto2"],
            "price": [100, 200],
            "url": ["/produto1", "/produto2"],
            "embedding": [[1, 0, 0], [0, 1, 0]]  # Embeddings ortogonais
        })
        
        df_bemol = pd.DataFrame({
            "title": ["produto3", "produto4"],
            "price": [150, 250],
            "url": ["/produto3", "/produto4"],
            "embedding": [[0.5, 0.5, 0], [0, 0.5, 0.5]]
        })
        
        # Calcula similaridade
        sim_matrix, matched_indices, matched_scores = calcular_similaridade_embeddings(
            df_magalu, df_bemol
        )
        
        # Verifica resultados
        assert sim_matrix.shape == (2, 2)  # Matriz 2x2
        assert len(matched_indices) == 2
        assert len(matched_scores) == 2
        assert all(0 <= score <= 1 for score in matched_scores)  # Scores entre 0 e 1
    
    def test_dataframes_vazios(self):
        """Testa com DataFrames vazios"""
        df_magalu = pd.DataFrame(columns=["title", "price", "url", "embedding"])
        df_bemol = pd.DataFrame(columns=["title", "price", "url", "embedding"])
        
        with pytest.raises(Exception):
            calcular_similaridade_embeddings(df_magalu, df_bemol)


class TestCriarParesProdutos:
    """Testes para função de criação de pares de produtos"""
    
    def test_criacao_pares(self):
        """Testa criação de pares de produtos"""
        df_magalu = pd.DataFrame({
            "title": ["produto1", "produto2"],
            "price": [100, 200],
            "url": ["/produto1", "/produto2"]
        })
        
        df_bemol = pd.DataFrame({
            "title": ["produto3", "produto4"],
            "price": [150, 250],
            "url": ["/produto3", "/produto4"]
        })
        
        matched_indices = np.array([0, 1])  # Primeiro produto Magalu matcha com primeiro Bemol
        matched_scores = np.array([0.8, 0.9])
        
        result = criar_pares_produtos(df_magalu, df_bemol, matched_indices, matched_scores)
        
        # Verifica estrutura do resultado
        assert len(result) == 4  # 2 produtos Magalu + 2 produtos Bemol
        
        # Verifica produtos do Magalu
        magalu_products = [r for r in result if r["marketplace"] == "Magalu"]
        assert len(magalu_products) == 2
        assert magalu_products[0]["title"] == "produto1"
        assert magalu_products[0]["similaridade"] == 0.8
        
        # Verifica produtos da Bemol
        bemol_products = [r for r in result if r["marketplace"] == "Bemol"]
        assert len(bemol_products) == 2
        assert bemol_products[0]["title"] == "produto3"
        assert bemol_products[0]["similaridade"] == 0.8
        
        # Verifica URLs
        assert "https://www.magazineluiza.com.br" in magalu_products[0]["url"]
        assert "https://www.bemol.com.br" in bemol_products[0]["url"]


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


class TestProcessarEmbeddingsCompletos:
    """Testes para função de processamento completo de embeddings"""
    
    @patch('src.embeddings.preparar_dataframe_embeddings')
    @patch('src.embeddings.calcular_similaridade_embeddings')
    @patch('src.embeddings.criar_pares_produtos')
    @patch('src.embeddings.identificar_produtos_exclusivos')
    @patch('src.embeddings.calcular_diferenca_precos_pares')
    @patch('src.embeddings.classificar_similaridade')
    def test_processamento_completo(self, mock_classificar, mock_diferenca, mock_exclusivos, 
                                   mock_pares, mock_similaridade, mock_preparar):
        """Testa processamento completo com mocks"""
        # Configura mocks
        df_magalu = pd.DataFrame({"title": ["produto1"], "price": [100], "url": ["/produto1"]})
        df_bemol = pd.DataFrame({"title": ["produto2"], "price": [150], "url": ["/produto2"]})
        
        mock_preparar.side_effect = [df_magalu, df_bemol]
        mock_similaridade.return_value = (np.array([[0.8]]), np.array([0]), np.array([0.8]))
        mock_pares.return_value = [
            {"title": "produto1", "marketplace": "Magalu", "price": 100, "url": "url1", "exclusividade": "não", "similaridade": 0.8},
            {"title": "produto2", "marketplace": "Bemol", "price": 150, "url": "url2", "exclusividade": "não", "similaridade": 0.8}
        ]
        mock_exclusivos.return_value = mock_pares.return_value
        mock_diferenca.return_value = pd.DataFrame(mock_pares.return_value)
        mock_classificar.return_value = "muito similar"
        
        # Executa função
        result = processar_embeddings_completos(df_magalu, df_bemol)
        
        # Verifica resultado
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "nivel_similaridade" in result.columns
        assert "diferenca_percentual" in result.columns
        
        # Verifica se mocks foram chamados
        mock_preparar.assert_called()
        mock_similaridade.assert_called()
        mock_pares.assert_called()
        mock_exclusivos.assert_called()
        mock_diferenca.assert_called()
        mock_classificar.assert_called()


class TestIntegracaoEmbeddings:
    """Testes de integração para módulo de embeddings"""
    
    def test_fluxo_completo_simples(self):
        """Testa fluxo completo com dados simples"""
        # Cria dados de teste
        df_magalu = pd.DataFrame({
            "title": ["Smartphone Samsung"],
            "price": ["R$ 1.299,00"],
            "url": ["/smartphone-samsung"],
            "embedding": [[0.1, 0.2, 0.3, 0.4, 0.5]]
        })
        
        df_bemol = pd.DataFrame({
            "title": ["Smartphone Samsung Galaxy"],
            "price": ["R$ 1.350,00"],
            "url": ["/smartphone-samsung-galaxy"],
            "embedding": [[0.1, 0.2, 0.3, 0.4, 0.6]]  # Similar mas não idêntico
        })
        
        # Executa processamento
        result = processar_embeddings_completos(df_magalu, df_bemol)
        
        # Verifica resultado
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 2  # Pelo menos os dois produtos originais
        assert "title" in result.columns
        assert "marketplace" in result.columns
        assert "price" in result.columns
        assert "url" in result.columns
        assert "exclusividade" in result.columns
        assert "similaridade" in result.columns
        assert "nivel_similaridade" in result.columns


if __name__ == "__main__":
    pytest.main([__file__]) 