"""Etapa 5: montagem dos agregados (documentos) a partir da zona confiável.

Segue o modelo de agregados da N2:
  corridas  um documento por GP, com circuito, resultados, classificação e pit stops embutidos
  pilotos   um documento por piloto, com o resumo da carreira calculado
  equipes   um documento por equipe, com o resumo histórico calculado
  voltas    um documento por piloto em cada corrida, com a lista de voltas
Os DataFrames aninhados são gravados em Parquet em data/aggregates/f1/<colecao>.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config


def _ler(spark: SparkSession, cfg: Config, nome: str) -> DataFrame:
    return spark.read.parquet(str(cfg.trusted / nome))


def montar_corridas(spark: SparkSession, cfg: Config) -> DataFrame:
    races, circuits = _ler(spark, cfg, "races"), _ler(spark, cfg, "circuits")
    results, status = _ler(spark, cfg, "results"), _ler(spark, cfg, "status")
    drivers, constructors = _ler(spark, cfg, "drivers"), _ler(spark, cfg, "constructors")
    qualifying, pit_stops = _ler(spark, cfg, "qualifying"), _ler(spark, cfg, "pit_stops")

    # Classificação: no máximo um registro por piloto em cada corrida.
    quali = (qualifying
             .groupBy("raceId", "driverId")
             .agg(F.min(F.struct(
                 F.col("position").alias("posicao"), "q1", "q2", "q3")).alias("classificacao")))

    # Pit stops de cada piloto em cada corrida, em ordem de parada.
    pits = (pit_stops
            .groupBy("raceId", "driverId")
            .agg(F.sort_array(F.collect_list(F.struct(
                F.col("stop").alias("parada"),
                F.col("lap").alias("volta"),
                F.col("milliseconds").alias("ms")))).alias("pitStops")))

    pilotos = drivers.select(
        "driverId",
        F.concat_ws(" ", "forename", "surname").alias("pilotoNome"),
        F.col("code").alias("pilotoCodigo"))
    equipes = constructors.select("constructorId", F.col("name").alias("equipeNome"))

    resultado = (results
                 .join(F.broadcast(pilotos), "driverId")
                 .join(F.broadcast(equipes), "constructorId")
                 .join(F.broadcast(status), "statusId")
                 .join(quali, ["raceId", "driverId"], "left")
                 .join(pits, ["raceId", "driverId"], "left")
                 .select(
                     "raceId",
                     F.struct(
                         # 'ordem' vem primeiro para a lista sair ordenada pela posição final.
                         F.col("positionOrder").alias("ordem"),
                         F.struct(F.col("driverId").alias("id"),
                                  F.col("pilotoNome").alias("nome"),
                                  F.col("pilotoCodigo").alias("codigo")).alias("piloto"),
                         F.struct(F.col("constructorId").alias("id"),
                                  F.col("equipeNome").alias("nome")).alias("equipe"),
                         F.col("grid"),
                         F.col("position").alias("posicao"),
                         F.col("points").alias("pontos"),
                         F.col("laps").alias("voltas"),
                         F.col("milliseconds").alias("tempoMs"),
                         F.col("fastestLapTime").alias("melhorVolta"),
                         F.col("status"),
                         F.col("classificacao"),
                         F.coalesce(F.col("pitStops"),
                                    F.array().cast(pits.schema["pitStops"].dataType)
                                    ).alias("pitStops"),
                     ).alias("resultado")))

    por_corrida = (resultado.groupBy("raceId")
                   .agg(F.sort_array(F.collect_list("resultado")).alias("resultados")))

    circuito = circuits.select(
        "circuitId",
        F.struct(F.col("circuitId").alias("id"), F.col("name").alias("nome"),
                 F.col("location").alias("cidade"), F.col("country").alias("pais"),
                 F.col("lat"), F.col("lng")).alias("circuito"))

    return (races
            .join(F.broadcast(circuito), "circuitId")
            .join(por_corrida, "raceId", "left")
            .select(
                F.col("raceId").alias("_id"),
                F.col("year").alias("ano"),
                F.col("round").alias("rodada"),
                F.col("name").alias("nome"),
                # Data e hora em UTC, como texto ISO; a carga converte para Date do BSON.
                F.concat(F.date_format("date", "yyyy-MM-dd"), F.lit("T"),
                         F.coalesce(F.col("time"), F.lit("00:00:00")), F.lit("Z")).alias("data"),
                "circuito",
                F.coalesce(F.col("resultados"),
                           F.array().cast(por_corrida.schema["resultados"].dataType)
                           ).alias("resultados")))


def _resumo(results: DataFrame, races: DataFrame, chave: str) -> DataFrame:
    """Resumo de carreira (piloto) ou histórico (equipe) a partir dos resultados."""
    com_ano = results.join(F.broadcast(races.select("raceId", "year")), "raceId")
    return com_ano.groupBy(chave).agg(F.struct(
        F.countDistinct("raceId").alias("corridas"),
        F.sum(F.when(F.col("position") == 1, 1).otherwise(0)).alias("vitorias"),
        F.sum(F.when(F.col("position") <= 3, 1).otherwise(0)).alias("podios"),
        F.round(F.sum(F.coalesce(F.col("points"), F.lit(0.0))), 2).alias("pontos"),
        F.min("year").alias("primeiraTemporada"),
        F.max("year").alias("ultimaTemporada"),
    ).alias("resumo"))


def montar_pilotos(spark: SparkSession, cfg: Config) -> DataFrame:
    drivers = _ler(spark, cfg, "drivers")
    resumo = _resumo(_ler(spark, cfg, "results"), _ler(spark, cfg, "races"), "driverId")
    return (drivers.join(resumo, "driverId", "left").select(
        F.col("driverId").alias("_id"),
        F.concat_ws(" ", "forename", "surname").alias("nome"),
        F.col("code").alias("codigo"),
        F.col("number").alias("numero"),
        F.date_format("dob", "yyyy-MM-dd").alias("nascimento"),
        F.col("nationality").alias("nacionalidade"),
        F.col("resumo").alias("carreira")))


def montar_equipes(spark: SparkSession, cfg: Config) -> DataFrame:
    constructors = _ler(spark, cfg, "constructors")
    resumo = _resumo(_ler(spark, cfg, "results"), _ler(spark, cfg, "races"), "constructorId")
    return (constructors.join(resumo, "constructorId", "left").select(
        F.col("constructorId").alias("_id"),
        F.col("name").alias("nome"),
        F.col("nationality").alias("nacionalidade"),
        F.col("resumo").alias("historico")))


def montar_voltas(spark: SparkSession, cfg: Config) -> DataFrame:
    lap_times = _ler(spark, cfg, "lap_times")
    return (lap_times.groupBy("raceId", "driverId")
            .agg(F.first("year").alias("ano"),
                 F.sort_array(F.collect_list(F.struct(
                     F.col("lap").alias("n"),
                     F.col("position").alias("posicao"),
                     F.col("milliseconds").alias("ms")))).alias("voltas"))
            .select(F.struct(F.col("raceId").alias("corridaId"),
                             F.col("driverId").alias("pilotoId")).alias("_id"),
                    "ano", "voltas"))


MONTADORES = {
    "corridas": montar_corridas,
    "pilotos": montar_pilotos,
    "equipes": montar_equipes,
    "voltas": montar_voltas,
}


def montar_agregados(spark: SparkSession, cfg: Config) -> dict[str, int]:
    """Monta as quatro coleções e grava em Parquet. Devolve a contagem de documentos."""
    contagens = {}
    for colecao, montar in MONTADORES.items():
        destino = cfg.aggregates / colecao
        montar(spark, cfg).write.mode("overwrite").parquet(str(destino))
        contagens[colecao] = spark.read.parquet(str(destino)).count()
        print(f"[agregados] {colecao:<9} {contagens[colecao]:>6} documentos")
    print(f"[agregados] gravados em: {cfg.aggregates}")
    return contagens
