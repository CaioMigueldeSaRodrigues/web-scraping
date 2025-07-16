import logging
import sys

# Garante que não haja handlers configurados no logger raiz que possam interferir
logging.basicConfig(level=logging.INFO, handlers=[])

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configura e retorna um logger com um handler que escreve para stdout.
    É idempotente, evitando a duplicação de handlers.
    """
    logger = logging.getLogger(name)
    
    # Se o logger já estiver configurado, não faça nada.
    if logger.hasHandlers():
        return logger

    logger.setLevel(level)
    logger.propagate = False  # Evita que o log seja passado para loggers ancestrais

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