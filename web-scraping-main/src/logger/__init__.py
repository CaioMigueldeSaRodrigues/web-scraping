import logging
import sys

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configura e retorna um logger com um handler que escreve para stdout.

    Args:
        name (str): O nome do logger, tipicamente __name__.
        level (int): O nível de logging (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: A instância do logger configurada.
    """
    # Evita adicionar handlers duplicados se a função for chamada múltiplas vezes
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    logger.setLevel(level)

    # Cria um handler que escreve para a saída padrão (stdout)
    handler = logging.StreamHandler(sys.stdout)
    
    # Define o formato da mensagem de log
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)

    # Adiciona o handler ao logger
    logger.addHandler(handler)

    return logger

# Exemplo de como usar (opcional, bom para testes)
if __name__ == '__main__':
    log = get_logger(__name__)
    log.info("Este é um log de informação.")
    log.warning("Este é um log de aviso.")
    log.error("Este é um log de erro.") 