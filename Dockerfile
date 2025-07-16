# Stage 1: Build com dependências
FROM python:3.9-slim as builder

WORKDIR /app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends build-essential

# Copiar arquivo de dependências
COPY requirements.txt .

# Instalar dependências em um ambiente virtual
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Imagem final
FROM python:3.9-slim

WORKDIR /app

# Copiar o ambiente virtual com as dependências do builder
COPY --from=builder /opt/venv /opt/venv

# Copiar o código da aplicação
COPY . .

# Ativar o ambiente virtual
ENV PATH="/opt/venv/bin:$PATH"

# Comando para rodar a aplicação
CMD ["python", "main.py"]
