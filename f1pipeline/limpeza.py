"""Etapas 3 e 4: validação e limpeza com Spark, gravação em Parquet (zona confiável).

Regras aplicadas a cada arquivo (as mesmas descritas na N2):
  * leitura com '\\N' tratado como nulo e textos sem espaços nas pontas;
  * conversão para o tipo certo (inteiro, decimal, data);
  * campos obrigatórios não podem ser nulos;
  * sem duplicatas pela chave;
  * integridade referencial (ex.: todo raceId de results existe em races);
  * colunas 'url' são descartadas.
Linhas que falham vão para a quarentena com o motivo da rejeição.
"""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import ARQUIVOS, Config

# (coluna, tipo, obrigatória)
ESPECIFICACAO: dict[str, dict] = {
    "circuits": {
        "chave": ["circuitId"],
        "colunas": [
            ("circuitId", "int", True), ("circuitRef", "string", False),
            ("name", "string", True), ("location", "string", False),
            ("country", "string", False), ("lat", "double", False),
            ("lng", "double", False), ("alt", "int", False),
        ],
        "fks": [],
    },
    "status": {
        "chave": ["statusId"],
        "colunas": [("statusId", "int", True), ("status", "string", True)],
        "fks": [],
    },
    "drivers": {
        "chave": ["driverId"],
        "colunas": [
            ("driverId", "int", True), ("driverRef", "string", False),
            ("number", "int", False), ("code", "string", False),
            ("forename", "string", True), ("surname", "string", True),
            ("dob", "date", False), ("nationality", "string", False),
        ],
        "fks": [],
    },
    "constructors": {
        "chave": ["constructorId"],
        "colunas": [
            ("constructorId", "int", True), ("constructorRef", "string", False),
            ("name", "string", True), ("nationality", "string", False),
        ],
        "fks": [],
    },
    "races": {
        "chave": ["raceId"],
        "colunas": [
            ("raceId", "int", True), ("year", "int", True), ("round", "int", True),
            ("circuitId", "int", True), ("name", "string", True),
            ("date", "date", True), ("time", "string", False),
        ],
        "fks": [("circuitId", "circuits", "circuitId")],
    },
    "results": {
        "chave": ["resultId"],
        "colunas": [
            ("resultId", "int", True), ("raceId", "int", True), ("driverId", "int", True),
            ("constructorId", "int", True), ("number", "int", False), ("grid", "int", False),
            ("position", "int", False), ("positionText", "string", False),
            ("positionOrder", "int", True), ("points", "double", False),
            ("laps", "int", False), ("time", "string", False),
            ("milliseconds", "long", False), ("fastestLap", "int", False),
            ("rank", "int", False), ("fastestLapTime", "string", False),
            ("fastestLapSpeed", "double", False), ("statusId", "int", True),
        ],
        "fks": [("raceId", "races", "raceId"), ("driverId", "drivers", "driverId"),
                ("constructorId", "constructors", "constructorId"),
                ("statusId", "status", "statusId")],
    },
    "qualifying": {
        "chave": ["qualifyId"],
        "colunas": [
            ("qualifyId", "int", True), ("raceId", "int", True), ("driverId", "int", True),
            ("constructorId", "int", True), ("number", "int", False),
            ("position", "int", False), ("q1", "string", False),
            ("q2", "string", False), ("q3", "string", False),
        ],
        "fks": [("raceId", "races", "raceId"), ("driverId", "drivers", "driverId"),
                ("constructorId", "constructors", "constructorId")],
    },
    "pit_stops": {
        "chave": ["raceId", "driverId", "stop"],
        "colunas": [
            ("raceId", "int", True), ("driverId", "int", True), ("stop", "int", True),
            ("lap", "int", False), ("time", "string", False),
            ("duration", "string", False), ("milliseconds", "long", False),
        ],
        "fks": [("raceId", "races", "raceId"), ("driverId", "drivers", "driverId")],
    },
    "lap_times": {
        "chave": ["raceId", "driverId", "lap"],
        "colunas": [
            ("raceId", "int", True), ("driverId", "int", True), ("lap", "int", True),
            ("position", "int", False), ("time", "string", False),
            ("milliseconds", "long", True),
        ],
        "fks": [("raceId", "races", "raceId"), ("driverId", "drivers", "driverId")],
    },
}


def _ler_csv(spark: SparkSession, caminho: Path) -> DataFrame:
    """Lê tudo como texto; a tipagem é feita depois, para poder registrar o que falhou."""
    return spark.read.csv(str(caminho), header=True, nullValue="\\N",
                          quote='"', escape='"', inferSchema=False)


def _validar(bruto: DataFrame, nome: str, validos: dict[str, DataFrame]) -> DataFrame:
    """Devolve o DataFrame com as colunas originais (texto), as tipadas (prefixo __)
    e a coluna 'motivo' (nula quando a linha é válida)."""
    spec = ESPECIFICACAO[nome]
    colunas = spec["colunas"]
    ausentes = [c for c, _, _ in colunas if c not in bruto.columns]
    if ausentes:
        raise ValueError(f"{nome}.csv sem as colunas esperadas: {ausentes}")

    # Texto aparado; string vazia vira nulo.
    df = bruto.select(*[
        F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c)
        for c, _, _ in colunas
    ])

    motivos = []
    for c, tipo, obrigatoria in colunas:
        if tipo == "string":
            convertida = F.col(c)
        elif tipo == "date":
            convertida = F.to_date(F.col(c), "yyyy-MM-dd")
        else:
            convertida = F.col(c).cast(tipo)
        df = df.withColumn(f"__{c}", convertida)
        if tipo != "string":
            motivos.append(F.when(F.col(c).isNotNull() & F.col(f"__{c}").isNull(),
                                  F.lit(f"tipo_invalido:{c}")))
        if obrigatoria:
            motivos.append(F.when(F.col(f"__{c}").isNull(),
                                  F.lit(f"obrigatorio_nulo:{c}")))

    # Duplicatas pela chave: mantém a primeira ocorrência.
    janela = Window.partitionBy(*[F.col(f"__{c}") for c in spec["chave"]]).orderBy(F.lit(1))
    df = df.withColumn("__rn", F.row_number().over(janela))
    motivos.append(F.when(F.col("__rn") > 1, F.lit("duplicado")))

    # Integridade referencial contra as entidades já limpas.
    for coluna, pai, coluna_pai in spec["fks"]:
        chaves_pai = (validos[pai]
                      .select(F.col(coluna_pai).alias(f"__pai_{coluna}"))
                      .distinct()
                      .withColumn(f"__ok_{coluna}", F.lit(True)))
        df = df.join(F.broadcast(chaves_pai),
                     df[f"__{coluna}"] == chaves_pai[f"__pai_{coluna}"], "left")
        motivos.append(F.when(F.col(f"__{coluna}").isNotNull() & F.col(f"__ok_{coluna}").isNull(),
                              F.lit(f"chave_orfa:{coluna}")))

    return df.withColumn("motivo", F.coalesce(*motivos))


def limpar(spark: SparkSession, cfg: Config, data_carga: str | None = None) -> dict:
    """Limpa os nove arquivos da carga e grava Parquet + quarentena. Devolve o relatório."""
    data_carga = data_carga or cfg.ultima_carga()
    raw_dir = cfg.raw_da_carga(data_carga)
    validos: dict[str, DataFrame] = {}
    relatorio = {"data_carga": data_carga, "arquivos": []}

    for nome in ARQUIVOS:
        colunas = [c for c, _, _ in ESPECIFICACAO[nome]["colunas"]]
        df = _validar(_ler_csv(spark, raw_dir / f"{nome}.csv"), nome, validos).cache()

        ok = df.where(F.col("motivo").isNull()).select(
            *[F.col(f"__{c}").alias(c) for c in colunas])
        rejeitado = df.where(F.col("motivo").isNotNull()).select(*colunas, "motivo")

        lidos = df.count()
        n_rejeitados = rejeitado.count()

        destino = cfg.trusted / nome
        if nome == "lap_times":
            # Maior volume: particionado por ano da corrida.
            anos = validos["races"].select(F.col("raceId").alias("__r"), "year")
            ok = ok.join(F.broadcast(anos), ok["raceId"] == anos["__r"]).drop("__r")
            ok.write.mode("overwrite").partitionBy("year").parquet(str(destino))
        else:
            ok.write.mode("overwrite").parquet(str(destino))

        motivos = {}
        if n_rejeitados:
            (rejeitado.coalesce(1).write.mode("overwrite").option("header", True)
             .csv(str(cfg.quarantine / data_carga / nome)))
            motivos = {r["motivo"]: r["count"]
                       for r in rejeitado.groupBy("motivo").count().collect()}

        df.unpersist()
        validos[nome] = spark.read.parquet(str(destino))
        relatorio["arquivos"].append({
            "arquivo": f"{nome}.csv", "lidos": lidos,
            "validos": lidos - n_rejeitados, "rejeitados": n_rejeitados,
            "motivos": motivos,
        })
        print(f"[limpeza] {nome:<13} lidos={lidos:>7}  válidos={lidos - n_rejeitados:>7}  "
              f"rejeitados={n_rejeitados:>5}  {motivos if motivos else ''}")

    cfg.reports.mkdir(parents=True, exist_ok=True)
    (cfg.reports / f"limpeza_{data_carga}.json").write_text(
        json.dumps(relatorio, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[limpeza] zona confiável: {cfg.trusted}")
    return relatorio
