"""Criação da sessão Spark em modo local."""
from __future__ import annotations

from pyspark.sql import SparkSession


def criar_spark(app_name: str = "f1-bigdata-pipeline") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        # Conversões inválidas viram NULL (e vão para a quarentena) em vez de abortar o job.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
