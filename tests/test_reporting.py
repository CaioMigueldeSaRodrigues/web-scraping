import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.reporting import (
    exportar_para_excel,
    gerar_relatorio_html,
    criar_tempview_databricks,
    gerar_relatorio_benchmarking,
    obter_estatisticas_relatorio,
    enviar_email_relatorio
)


class TestExportarParaExcel:
    """Testes para função de exportação Excel"""
    
    def test_exportacao_basica(self):
        """Testa exportação básica para Excel"""
        df = pd.DataFrame({
            "title": ["produto1", "produto2"],
            "price": [100, 200],
            "url": ["http://test.com/1", "http://test.com/2"],
            "similaridade": [0.8, 0.9],
            "nivel_similaridade": ["muito similar", "muito similar"],
            "diferenca_percentual": [10.5, 15.2]
        })
        
        with patch('builtins.open', create=True):
            caminho = exportar_para_excel(df, "teste.xlsx")
            assert caminho == "teste.xlsx"
    
    def test_exportacao_com_colunas_ocultas(self):
        """Testa exportação com colunas ocultas"""
        df = pd.DataFrame({
            "title": ["produto1"],
            "price": [100],
            "url": ["http://test.com/1"],
            "similaridade": [0.8],
            "nivel_similaridade": ["muito similar"]
        })
        
        colunas_ocultas = ["similaridade", "nivel_similaridade"]
        
        with patch('builtins.open', create=True):
            caminho = exportar_para_excel(df, "teste.xlsx", colunas_ocultas)
            assert caminho == "teste.xlsx"


class TestGerarRelatorioHtml:
    """Testes para função de geração de relatório HTML"""
    
    def test_geracao_html_basica(self):
        """Testa geração básica de HTML"""
        df = pd.DataFrame({
            "title": ["produto1", "produto2"],
            "price": [100, 200],
            "url": ["http://test.com/1", "http://test.com/2"],
            "similaridade": [0.8, 0.9],
            "nivel_similaridade": ["muito similar", "muito similar"]
        })
        
        with patch('src.reporting.dbutils') as mock_dbutils:
            caminho = gerar_relatorio_html(df, "/dbfs/teste.html")
            assert caminho == "/dbfs/teste.html"
            mock_dbutils.fs.put.assert_called_once()
    
    def test_geracao_html_com_colunas_ocultas(self):
        """Testa geração HTML com colunas ocultas"""
        df = pd.DataFrame({
            "title": ["produto1"],
            "price": [100],
            "url": ["http://test.com/1"],
            "similaridade": [0.8],
            "nivel_similaridade": ["muito similar"]
        })
        
        colunas_ocultas = ["similaridade", "nivel_similaridade"]
        
        with patch('src.reporting.dbutils') as mock_dbutils:
            caminho = gerar_relatorio_html(df, "/dbfs/teste.html", colunas_ocultas)
            assert caminho == "/dbfs/teste.html"


class TestCriarTempviewDatabricks:
    """Testes para função de criação de TempView"""
    
    def test_criacao_tempview(self):
        """Testa criação de TempView"""
        df = pd.DataFrame({
            "title": ["produto1"],
            "price": [100],
            "url": ["http://test.com/1"]
        })
        
        with patch('src.reporting.spark') as mock_spark:
            mock_spark.createDataFrame.return_value.createOrReplaceTempView.return_value = None
            
            nome = criar_tempview_databricks(df, "test_tempview")
            assert nome == "test_tempview"
            mock_spark.createDataFrame.assert_called_once()


class TestGerarRelatorioBenchmarking:
    """Testes para função de geração de relatório completo"""
    
    @patch('src.reporting.criar_tempview_databricks')
    @patch('src.reporting.exportar_para_excel')
    @patch('src.reporting.gerar_relatorio_html')
    def test_geracao_relatorio_completo(self, mock_html, mock_excel, mock_tempview):
        """Testa geração completa de relatório"""
        df = pd.DataFrame({
            "title": ["produto1"],
            "price": [100],
            "url": ["http://test.com/1"],
            "exclusividade": ["não"]
        })
        
        mock_tempview.return_value = "test_tempview"
        mock_excel.return_value = "teste.xlsx"
        mock_html.return_value = "/dbfs/teste.html"
        
        excel_path, html_path, tempview_name = gerar_relatorio_benchmarking(df)
        
        assert excel_path == "teste.xlsx"
        assert html_path == "/dbfs/teste.html"
        assert tempview_name == "test_tempview"


class TestObterEstatisticasRelatorio:
    """Testes para função de estatísticas"""
    
    def test_estatisticas_basicas(self):
        """Testa cálculo de estatísticas básicas"""
        df = pd.DataFrame({
            "title": ["produto1", "produto2", "produto3"],
            "marketplace": ["Magalu", "Bemol", "Magalu"],
            "price": [100, 150, 200],
            "exclusividade": ["não", "não", "sim"],
            "nivel_similaridade": ["muito similar", "moderadamente similar", "exclusivo"]
        })
        
        stats = obter_estatisticas_relatorio(df)
        
        assert stats["total_produtos"] == 3
        assert stats["produtos_magalu"] == 2
        assert stats["produtos_bemol"] == 1
        assert stats["produtos_exclusivos"] == 1
        assert stats["produtos_pareados"] == 2
        assert stats["muito_similar"] == 1
        assert stats["moderadamente_similar"] == 1
        assert stats["exclusivo"] == 1
    
    def test_estatisticas_vazio(self):
        """Testa estatísticas com DataFrame vazio"""
        df = pd.DataFrame()
        
        stats = obter_estatisticas_relatorio(df)
        
        assert stats["total_produtos"] == 0
        assert stats["produtos_magalu"] == 0
        assert stats["produtos_bemol"] == 0


class TestEnviarEmailRelatorio:
    """Testes para função de envio de email"""
    
    @patch('src.reporting.SendGridAPIClient')
    @patch('src.reporting.dbutils')
    def test_envio_email_sucesso(self, mock_dbutils, mock_sendgrid):
        """Testa envio de email com sucesso"""
        df = pd.DataFrame({
            "title": ["produto1"],
            "price": [100],
            "url": ["http://test.com/1"]
        })
        
        stats = {
            "total_produtos": 1,
            "produtos_pareados": 1,
            "produtos_exclusivos": 0,
            "muito_similar": 1,
            "moderadamente_similar": 0,
            "pouco_similar": 0,
            "exclusivo": 0
        }
        
        # Mock dbutils.secrets
        mock_dbutils.secrets.get.side_effect = ["api_key", "from@test.com", "to@test.com"]
        
        # Mock SendGrid
        mock_sg_instance = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_sg_instance.send.return_value = mock_response
        mock_sendgrid.return_value = mock_sg_instance
        
        # Mock open para anexo
        with patch('builtins.open', create=True):
            resultado = enviar_email_relatorio(
                df, stats, "teste.xlsx", "/dbfs/teste.html"
            )
        
        assert resultado is True
        mock_sg_instance.send.assert_called_once()
    
    @patch('src.reporting.dbutils')
    def test_envio_email_falha_configuracao(self, mock_dbutils):
        """Testa envio de email com falha na configuração"""
        df = pd.DataFrame({"title": ["produto1"]})
        stats = {"total_produtos": 1}
        
        # Mock dbutils.secrets para falhar
        mock_dbutils.secrets.get.side_effect = Exception("Secret não encontrado")
        
        resultado = enviar_email_relatorio(
            df, stats, "teste.xlsx", "/dbfs/teste.html"
        )
        
        # Deve usar configurações padrão e tentar enviar
        assert resultado is False
    
    @patch('src.reporting.SendGridAPIClient')
    @patch('src.reporting.dbutils')
    def test_envio_email_falha_envio(self, mock_dbutils, mock_sendgrid):
        """Testa falha no envio de email"""
        df = pd.DataFrame({"title": ["produto1"]})
        stats = {"total_produtos": 1}
        
        # Mock dbutils.secrets
        mock_dbutils.secrets.get.side_effect = ["api_key", "from@test.com", "to@test.com"]
        
        # Mock SendGrid para falhar
        mock_sendgrid.side_effect = Exception("Erro de conexão")
        
        resultado = enviar_email_relatorio(
            df, stats, "teste.xlsx", "/dbfs/teste.html"
        )
        
        assert resultado is False


class TestIntegracaoReporting:
    """Testes de integração para módulo de reporting"""
    
    def test_fluxo_completo_reporting(self):
        """Testa fluxo completo de reporting"""
        df = pd.DataFrame({
            "title": ["Smartphone Samsung", "Smartphone iPhone"],
            "marketplace": ["Magalu", "Bemol"],
            "price": [1299.0, 1350.0],
            "url": ["http://magalu.com/1", "http://bemol.com/1"],
            "exclusividade": ["não", "não"],
            "similaridade": [0.92, 0.92],
            "nivel_similaridade": ["muito similar", "muito similar"],
            "diferenca_percentual": [8.5, 8.5]
        })
        
        # Testa estatísticas
        stats = obter_estatisticas_relatorio(df)
        assert stats["total_produtos"] == 2
        assert stats["produtos_pareados"] == 2
        assert stats["muito_similar"] == 2
        
        # Testa exportação Excel (mock)
        with patch('builtins.open', create=True):
            caminho_excel = exportar_para_excel(df, "teste.xlsx")
            assert caminho_excel == "teste.xlsx"
        
        # Testa geração HTML (mock)
        with patch('src.reporting.dbutils') as mock_dbutils:
            caminho_html = gerar_relatorio_html(df, "/dbfs/teste.html")
            assert caminho_html == "/dbfs/teste.html"


if __name__ == "__main__":
    pytest.main([__file__]) 