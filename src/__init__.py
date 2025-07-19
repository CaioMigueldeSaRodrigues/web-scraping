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

from .main import (
    executar_pipeline_benchmarking,
    executar_pipeline_completo,
    executar_pipeline_com_email,
    executar_pipeline_completo_com_email,
    validar_parametros_pipeline,
    listar_tabelas_disponiveis
)

from .data_processing import (
    limpar_precos,
    construir_urls,
    identificar_produtos_exclusivos,
    calcular_diferenca_precos_pares,
    classificar_similaridade,
    remover_duplicados_por_marketplace,
    limpar_e_preparar_dataframe_final
)

from .embeddings import (
    processar_embeddings_completos,
    calcular_similaridade_embeddings
)

from .reporting import (
    gerar_relatorio_benchmarking,
    enviar_email_relatorio,
    obter_estatisticas_relatorio,
    exportar_relatorio_benchmarking_excel
)

from .config import (
    SIMILARIDADE_THRESHOLD,
    PRECO_THRESHOLD,
    EMAIL_CONFIG
)

from .logger_config import get_logger

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