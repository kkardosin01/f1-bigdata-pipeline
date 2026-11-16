# Imagem com Java 17 + Python para rodar o pipeline sem instalar nada na máquina.
FROM python:3.12-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY f1pipeline ./f1pipeline
ENTRYPOINT ["python", "-m", "f1pipeline"]
