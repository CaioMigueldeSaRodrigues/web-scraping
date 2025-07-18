import re
import pandas as pd
import numpy as np
from typing import Union, Optional, List, Dict, Any
from .logger_config import get_logger

logger = get_logger(__name__)


def limpar_preco(preco: Union[str, float, int]) -> float:
    """
    Limpa e converte preços em formato brasileiro para float.
    
    Args:
        preco: Preço em formato string, float ou int
        
    Returns:
        float: Preço limpo como float, 0.0 se inválido
    """
    try:
        preco_str = str(preco)
        
        # Remove caracteres especiais invisíveis e palavras
        preco_str = preco_str.replace('\xa0', '').replace('R$', '').replace('ou', '')
        preco_str = preco_str.strip()
        
        # Captura padrão tipo 6.599,00
        match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', preco_str)
        if match:
            preco_str = match.group(1).replace('.', '').replace(',', '.')
            return float(preco_str)
            
        # Se não bateu regex, força substituição bruta
        preco_str = preco_str.replace('.', '').replace(',', '.')
        return float(preco_str)
        
    except Exception as e:
        logger.warning(f"Erro ao limpar preço '{preco}': {e}")
        return 0.0


def classificar_similaridade(score: float) -> str:
    """
    Classifica score de similaridade em níveis qualitativos.
    
    Args:
        score: Score de similaridade (0-1 ou -1 para exclusivo)
        
    Returns:
        str: Nível de similaridade
    """
    if score == -1:
        return "exclusivo"
    elif score >= 0.85:
        return "muito similar"
    elif score >= 0.5:
        return "moderadamente similar"
    else:
        return "pouco similar"


def percentual_diferenca(p1: float, p2: float) -> Optional[float]:
    """
    Calcula diferença percentual entre dois preços.
    
    Args:
        p1: Primeiro preço
        p2: Segundo preço
        
    Returns:
        Optional[float]: Diferença percentual ou None se erro
    """
    try:
        if p1 == 0 or p2 == 0:
            return None
        return abs(p1 - p2) / ((p1 + p2) / 2) * 100
    except Exception as e:
        logger.warning(f"Erro ao calcular diferença percentual: {e}")
        return None


def preparar_dataframe_embeddings(df: pd.DataFrame, marketplace: str) -> pd.DataFrame:
    """
    Prepara DataFrame com embeddings para processamento.
    
    Args:
        df: DataFrame com embeddings
        marketplace: Nome do marketplace
        
    Returns:
        pd.DataFrame: DataFrame preparado
    """
    try:
        df_copy = df.copy()
        df_copy["marketplace"] = marketplace
        df_copy["price"] = df_copy["price"].apply(limpar_preco)
        df_copy["embedding"] = df_copy["embedding"].apply(np.array)
        
        # Remove linhas com embeddings nulos
        df_copy = df_copy[df_copy["embedding"].notnull()]
        
        logger.info(f"DataFrame {marketplace} preparado: {len(df_copy)} produtos")
        return df_copy
        
    except Exception as e:
        logger.error(f"Erro ao preparar DataFrame {marketplace}: {e}")
        raise


def construir_url_completa(url: str, base_url: str) -> str:
    """
    Constrói URL completa baseada na URL base.
    
    Args:
        url: URL relativa ou absoluta
        base_url: URL base do marketplace
        
    Returns:
        str: URL completa
    """
    if not str(url).startswith("http"):
        return f"{base_url}{url}"
    return url


def identificar_produtos_exclusivos(
    df_magalu: pd.DataFrame, 
    df_bemol: pd.DataFrame, 
    result: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Identifica produtos exclusivos de cada marketplace.
    
    Args:
        df_magalu: DataFrame do Magalu
        df_bemol: DataFrame da Bemol
        result: Lista de resultados existente
        
    Returns:
        List[Dict[str, Any]]: Lista atualizada com produtos exclusivos
    """
    try:
        # Identifica produtos já processados
        titulos_magalu = {r["title"] for r in result if r["marketplace"] == "Magalu"}
        titulos_bemol = {r["title"] for r in result if r["marketplace"] == "Bemol"}
        
        # Adiciona produtos exclusivos do Magalu
        for row in df_magalu[~df_magalu["title"].isin(titulos_magalu)].itertuples():
            url = construir_url_completa(row.url, "https://www.magazineluiza.com.br")
            result.append({
                "title": row.title,
                "marketplace": "Magalu",
                "price": row.price,
                "url": url,
                "exclusividade": "sim",
                "similaridade": -1
            })
        
        # Adiciona produtos exclusivos da Bemol
        for row in df_bemol[~df_bemol["title"].isin(titulos_bemol)].itertuples():
            url = construir_url_completa(row.url, "https://www.bemol.com.br")
            result.append({
                "title": row.title,
                "marketplace": "Bemol",
                "price": row.price,
                "url": url,
                "exclusividade": "sim",
                "similaridade": -1
            })
            
        logger.info(f"Produtos exclusivos identificados: {len(result) - len(titulos_magalu) - len(titulos_bemol)}")
        return result
        
    except Exception as e:
        logger.error(f"Erro ao identificar produtos exclusivos: {e}")
        raise


def calcular_diferenca_precos_pares(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula diferença percentual de preços entre pares de produtos.
    
    Args:
        df: DataFrame com produtos pareados
        
    Returns:
        pd.DataFrame: DataFrame com diferença percentual calculada
    """
    try:
        df_copy = df.copy()
        
        # Calcula diferença percentual para pares
        df_copy["diferenca_percentual"] = df_copy.groupby(df_copy.index // 2)["price"].transform(
            lambda x: percentual_diferenca(x.iloc[0], x.iloc[1]) if len(x) == 2 else None
        )
        
        logger.info("Diferença percentual de preços calculada")
        return df_copy
        
    except Exception as e:
        logger.error(f"Erro ao calcular diferença de preços: {e}")
        raise


def remover_duplicados_por_marketplace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicados considerando apenas duplicação dentro de cada marketplace.
    Mantém o produto mais barato em caso de duplicação.
    
    Args:
        df: DataFrame com produtos
        
    Returns:
        pd.DataFrame: DataFrame sem duplicados
    """
    try:
        df_copy = df.copy()
        
        # Ordena por preço crescente para manter o mais barato
        df_copy = df_copy.sort_values(by="price", ascending=True)
        
        # Remove duplicados mantendo o primeiro (mais barato)
        df_copy = df_copy.drop_duplicates(
            subset=["title", "marketplace"], 
            keep="first"
        ).reset_index(drop=True)
        
        logger.info(f"Duplicados removidos: {len(df) - len(df_copy)} produtos")
        return df_copy
        
    except Exception as e:
        logger.error(f"Erro ao remover duplicados: {e}")
        raise


def definir_colunas_relatorio(df: pd.DataFrame) -> tuple:
    """
    Define colunas visíveis e ocultas para relatório.
    
    Args:
        df: DataFrame com dados completos
        
    Returns:
        tuple: (colunas_visiveis, colunas_ocultas)
    """
    colunas_ocultas = ["similaridade", "nivel_similaridade", "diferenca_percentual"]
    colunas_visiveis = [col for col in df.columns if col not in colunas_ocultas]
    
    return colunas_visiveis, colunas_ocultas


def limpar_e_preparar_dataframe_final(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica limpeza completa no DataFrame final.
    
    Args:
        df: DataFrame com dados processados
        
    Returns:
        pd.DataFrame: DataFrame limpo e preparado
    """
    try:
        df_copy = df.copy()
        
        # Remove duplicados por marketplace
        df_copy = remover_duplicados_por_marketplace(df_copy)
        
        # Ordena por exclusividade e similaridade
        df_copy = df_copy.sort_values(
            by=["exclusividade", "similaridade"], 
            ascending=[True, False]
        ).reset_index(drop=True)
        
        # Adiciona classificação de similaridade
        df_copy["nivel_similaridade"] = df_copy["similaridade"].apply(classificar_similaridade)
        
        # Calcula diferença percentual de preços
        df_copy = calcular_diferenca_precos_pares(df_copy)
        
        logger.info(f"DataFrame final limpo e preparado: {len(df_copy)} produtos")
        return df_copy
        
    except Exception as e:
        logger.error(f"Erro ao limpar DataFrame final: {e}")
        raise 