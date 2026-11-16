"""Configuração central do pipeline: caminhos das zonas, dataset e MongoDB.

Tudo pode ser sobrescrito por variáveis de ambiente (ver .env.example).
"""
from __future__ import annotations

import os
from pathlib import Path

# Dataset de origem no Kaggle (especificado na N2).
KAGGLE_DATASET = "rohanrao/formula-1-world-championship-1950-2020"

# Os nove arquivos CSV usados pelo pipeline, na ordem em que são limpos:
# primeiro as entidades "pai", depois as que dependem delas (integridade referencial).
ARQUIVOS = [
    "circuits",
    "status",
    "drivers",
    "constructors",
    "races",
    "results",
    "qualifying",
    "pit_stops",
    "lap_times",
]

COLECOES = ["corridas", "pilotos", "equipes", "voltas"]


class Config:
    """Caminhos das zonas de dados e parâmetros de conexão."""

    def __init__(self, base_dir: str | os.PathLike | None = None,
                 mongo_uri: str | None = None, mongo_db: str | None = None):
        self.base = Path(base_dir or os.getenv("F1_DATA_DIR", "data")).resolve()
        self.raw = self.base / "raw" / "f1"                # zona bruta: CSV original
        self.trusted = self.base / "trusted" / "f1"        # zona confiável: Parquet limpo
        self.quarantine = self.base / "quarantine" / "f1"  # linhas rejeitadas + motivo
        self.aggregates = self.base / "aggregates" / "f1"  # documentos prontos (Parquet aninhado)
        self.reports = self.base / "reports"               # relatórios de execução (JSON)
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.mongo_db = mongo_db or os.getenv("MONGO_DB", "f1")

    def raw_da_carga(self, data_carga: str) -> Path:
        return self.raw / data_carga

    def ultima_carga(self) -> str:
        """Data (nome da pasta) da carga mais recente na zona bruta."""
        if not self.raw.exists():
            raise FileNotFoundError(
                f"Zona bruta vazia ({self.raw}). Rode a etapa de captura primeiro.")
        cargas = sorted(p.name for p in self.raw.iterdir() if p.is_dir())
        if not cargas:
            raise FileNotFoundError(
                f"Nenhuma carga em {self.raw}. Rode a etapa de captura primeiro.")
        return cargas[-1]
