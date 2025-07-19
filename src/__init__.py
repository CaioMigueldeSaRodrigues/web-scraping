"""
Web Scraping Pipeline - Bemol vs Magalu Benchmarking

Este pacote contém o pipeline completo de benchmarking entre Bemol e Magazine Luiza,
incluindo web scraping, processamento de embeddings, análise de similaridade,
geração de relatórios e envio de emails.

Módulos principais:
- main: Funções principais do pipeline
- data_processing: Processamento e limpeza de dados
- embeddings: Geração e comparação de embeddings
- reporting: Geração de relatórios e envio de emails
- config: Configurações do projeto
- logger_config: Configuração de logging
"""

# Imports principais com tratamento de erro
try:
    from .main import (
        executar_pipeline_benchmarking,
        executar_pipeline_completo,
        executar_pipeline_com_email,
        executar_pipeline_completo_com_email,
        validar_parametros_pipeline,
        listar_tabelas_disponiveis
    )
except ImportError as e:
    print(f"⚠️ Aviso: Erro ao importar funções do módulo main: {e}")
    # Define funções placeholder se não conseguirem ser importadas
    def executar_pipeline_benchmarking(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def executar_pipeline_completo(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def executar_pipeline_com_email(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def executar_pipeline_completo_com_email(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def validar_parametros_pipeline(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def listar_tabelas_disponiveis(*args, **kwargs):
        raise NotImplementedError("Função não disponível")

# Imports de processamento de dados
try:
    from .data_processing import (
        limpar_precos,
        construir_urls,
        identificar_produtos_exclusivos,
        calcular_diferenca_precos_pares,
        classificar_similaridade,
        remover_duplicados_por_marketplace,
        limpar_e_preparar_dataframe_final
    )
except ImportError as e:
    print(f"⚠️ Aviso: Erro ao importar funções de data_processing: {e}")
    # Define funções placeholder
    def limpar_precos(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def construir_urls(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def identificar_produtos_exclusivos(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def calcular_diferenca_precos_pares(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def classificar_similaridade(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def remover_duplicados_por_marketplace(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def limpar_e_preparar_dataframe_final(*args, **kwargs):
        raise NotImplementedError("Função não disponível")

# Imports de embeddings
try:
    from .embeddings import (
        processar_embeddings_completos,
        calcular_similaridade_embeddings
    )
except ImportError as e:
    print(f"⚠️ Aviso: Erro ao importar funções de embeddings: {e}")
    def processar_embeddings_completos(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def calcular_similaridade_embeddings(*args, **kwargs):
        raise NotImplementedError("Função não disponível")

# Imports de reporting
try:
    from .reporting import (
        gerar_relatorio_benchmarking,
        enviar_email_relatorio,
        obter_estatisticas_relatorio,
        exportar_relatorio_benchmarking_excel
    )
except ImportError as e:
    print(f"⚠️ Aviso: Erro ao importar funções de reporting: {e}")
    def gerar_relatorio_benchmarking(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def enviar_email_relatorio(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def obter_estatisticas_relatorio(*args, **kwargs):
        raise NotImplementedError("Função não disponível")
    def exportar_relatorio_benchmarking_excel(*args, **kwargs):
        raise NotImplementedError("Função não disponível")

# Imports de configuração
try:
    from .config import (
        SIMILARIDADE_THRESHOLD,
        PRECO_THRESHOLD,
        EMAIL_CONFIG
    )
except ImportError as e:
    print(f"⚠️ Aviso: Erro ao importar configurações: {e}")
    SIMILARIDADE_THRESHOLD = 0.8
    PRECO_THRESHOLD = 0.1
    EMAIL_CONFIG = {}

# Imports de logging
try:
    from .logger_config import get_logger
except ImportError as e:
    print(f"⚠️ Aviso: Erro ao importar logger: {e}")
    def get_logger(*args, **kwargs):
        raise NotImplementedError("Logger não disponível")

__version__ = "1.0.0"
__author__ = "Caio Miguel de Sá Rodrigues"
__email__ = "caiomiguel@bemol.com.br"

__all__ = [
    # Main pipeline functions
    "executar_pipeline_benchmarking",
    "executar_pipeline_completo", 
    "executar_pipeline_com_email",
    "executar_pipeline_completo_com_email",
    "validar_parametros_pipeline",
    "listar_tabelas_disponiveis",
    
    # Data processing functions
    "limpar_precos",
    "construir_urls", 
    "identificar_produtos_exclusivos",
    "calcular_diferenca_precos_pares",
    "classificar_similaridade",
    "remover_duplicados_por_marketplace",
    "limpar_e_preparar_dataframe_final",
    
    # Embeddings functions
    "processar_embeddings_completos",
    "calcular_similaridade_embeddings",
    
    # Reporting functions
    "gerar_relatorio_benchmarking",
    "enviar_email_relatorio",
    "obter_estatisticas_relatorio", 
    "exportar_relatorio_benchmarking_excel",
    
    # Configuration
    "SIMILARIDADE_THRESHOLD",
    "PRECO_THRESHOLD", 
    "EMAIL_CONFIG",
    
    # Logging
    "get_logger"
] 