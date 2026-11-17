"""Linha de comando do pipeline.

Exemplos:
  python -m f1pipeline                          # todas as etapas, baixando do Kaggle
  python -m f1pipeline --csv-dir ~/Downloads/f1 # todas as etapas, CSVs já baixados
  python -m f1pipeline --etapa consultas        # só as consultas de demonstração
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date

from .config import Config

ETAPAS = ["captura", "limpeza", "agregados", "carga", "consultas"]


def _argumentos(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m f1pipeline",
        description="Pipeline de Big Data: Fórmula 1 (Kaggle) -> Parquet -> MongoDB")
    p.add_argument("--etapa", choices=["all", *ETAPAS], default="all",
                   help="etapa a executar (padrão: all)")
    p.add_argument("--csv-dir", help="pasta com os CSVs já baixados (pula o download do Kaggle)")
    p.add_argument("--data-carga", help="data da carga, AAAA-MM-DD (padrão: hoje na captura; "
                                        "a mais recente nas demais etapas)")
    p.add_argument("--data-dir", help="pasta das zonas de dados (padrão: ./data ou $F1_DATA_DIR)")
    p.add_argument("--mongo-uri", help="URI do MongoDB (padrão: $MONGO_URI ou "
                                       "mongodb://localhost:27017)")
    p.add_argument("--mongo-db", help="nome do banco (padrão: $MONGO_DB ou f1)")
    p.add_argument("--ano", type=int, default=2023,
                   help="temporada usada nas consultas 5 e 7 (padrão: 2023)")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = _argumentos(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)  # progresso aparece na hora
    cfg = Config(args.data_dir, args.mongo_uri, args.mongo_db)
    etapas = ETAPAS if args.etapa == "all" else [args.etapa]
    inicio = time.time()

    if "captura" in etapas:
        from .captura import capturar
        print("\n>>> Etapas 1-2: captura e zona bruta")
        args.data_carga = args.data_carga or date.today().isoformat()
        capturar(cfg, args.data_carga, args.csv_dir)  # a limpeza usa esta mesma carga

    spark = None
    if {"limpeza", "agregados", "carga"} & set(etapas):
        from .spark import criar_spark
        spark = criar_spark()

    try:
        if "limpeza" in etapas:
            from .limpeza import limpar
            print("\n>>> Etapas 3-4: limpeza com Spark e zona confiável (Parquet)")
            limpar(spark, cfg, args.data_carga)

        if "agregados" in etapas:
            from .agregados import montar_agregados
            print("\n>>> Etapa 5: montagem dos agregados")
            montar_agregados(spark, cfg)

        db = None
        if {"carga", "consultas"} & set(etapas):
            from .carga import conectar
            db = conectar(cfg.mongo_uri)[cfg.mongo_db]

        if "carga" in etapas:
            from .carga import carregar
            print(f"\n>>> Etapa 6: ingestão no MongoDB (banco '{cfg.mongo_db}')")
            carregar(spark, cfg, db)

        if "consultas" in etapas:
            from .consultas import executar_consultas
            print("\n>>> Etapa 7: consultas de demonstração")
            executar_consultas(db, ano=args.ano)
    finally:
        if spark is not None:
            spark.stop()

    print(f"Concluído em {time.time() - inicio:.1f} s.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
