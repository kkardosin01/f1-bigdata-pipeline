"""Etapa 6: ingestão dos agregados no MongoDB.

Cada documento é gravado com replace + upsert pelo _id, então repetir a carga
não duplica nada (carga idempotente). Os índices são criados ao final.
"""
from __future__ import annotations

from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient, ReplaceOne
from pyspark.sql import SparkSession

from .config import COLECOES, Config

LOTE = 1000

INDICES = {
    "corridas": [
        [("ano", ASCENDING), ("rodada", ASCENDING)],
        [("resultados.piloto.id", ASCENDING)],
        [("resultados.equipe.id", ASCENDING)],
    ],
    "voltas": [
        [("_id.pilotoId", ASCENDING), ("ano", ASCENDING)],
    ],
}


def conectar(uri: str):
    """Abre a conexão. 'mongomock://' usa um banco em memória (testes/demonstração)."""
    if uri.startswith("mongomock://"):
        import mongomock
        return mongomock.MongoClient()
    cliente = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    try:
        cliente.admin.command("ping")
    except Exception as exc:
        raise RuntimeError(
            f"Não foi possível conectar ao MongoDB em '{uri}'. Verifique se o servidor "
            "está no ar (ex.: 'docker compose up -d mongo') ou ajuste MONGO_URI.\n"
            f"Erro original: {exc}") from exc
    return cliente


def _sem_nulos(valor):
    """Remove campos nulos: no MongoDB o campo ausente simplesmente não existe."""
    if isinstance(valor, dict):
        return {k: _sem_nulos(v) for k, v in valor.items() if v is not None}
    if isinstance(valor, list):
        return [_sem_nulos(v) for v in valor]
    return valor


def _para_data(texto: str | None, formato: str):
    if not texto:
        return None
    try:
        return datetime.strptime(texto, formato).replace(tzinfo=timezone.utc)
    except ValueError:
        try:  # hora fora do padrão: guarda só a data
            return datetime.strptime(texto[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def _preparar(colecao: str, doc: dict) -> dict:
    """Ajustes de tipo que só fazem sentido no BSON (datas)."""
    if colecao == "corridas":
        doc["data"] = _para_data(doc.get("data"), "%Y-%m-%dT%H:%M:%SZ")
    elif colecao == "pilotos":
        doc["nascimento"] = _para_data(doc.get("nascimento"), "%Y-%m-%d")
    return _sem_nulos(doc)


def _gravar_lote(colecao, docs: list[dict]) -> None:
    """Upsert em lote pelo _id. No banco em memória (mongomock), que não aceita o
    bulk_write das versões novas do pymongo, grava documento a documento."""
    if type(colecao).__module__.startswith("mongomock"):
        try:
            colecao.insert_many(docs, ordered=True)
        except Exception:  # já existia algum _id: regrava um a um
            for doc in docs:
                colecao.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        return
    colecao.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs],
                       ordered=False)


def carregar(spark: SparkSession, cfg: Config, db) -> dict[str, int]:
    """Grava as quatro coleções e cria os índices. Devolve documentos por coleção."""
    contagens = {}
    for colecao in COLECOES:
        df = spark.read.parquet(str(cfg.aggregates / colecao))
        lote, gravados = [], 0
        for linha in df.toLocalIterator():
            lote.append(_preparar(colecao, linha.asDict(recursive=True)))
            if len(lote) >= LOTE:
                _gravar_lote(db[colecao], lote)
                gravados += len(lote)
                lote = []
        if lote:
            _gravar_lote(db[colecao], lote)
            gravados += len(lote)
        for indice in INDICES.get(colecao, []):
            db[colecao].create_index(indice)
        contagens[colecao] = db[colecao].count_documents({})
        print(f"[carga] {colecao:<9} gravados={gravados:>6}  total na coleção={contagens[colecao]:>6}")
    return contagens
