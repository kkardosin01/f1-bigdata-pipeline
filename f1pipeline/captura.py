"""Etapas 1 e 2: captura do dataset no Kaggle e gravação na zona bruta.

Os CSVs são copiados sem nenhuma alteração para data/raw/f1/<data_da_carga>/,
junto com um manifesto (_manifesto.json) com tamanho, linhas e hash de cada arquivo.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from .config import ARQUIVOS, KAGGLE_DATASET, Config


def _baixar_do_kaggle() -> Path:
    """Baixa o dataset com o kagglehub e devolve a pasta com os CSVs."""
    try:
        import kagglehub
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "kagglehub não instalado. Rode 'pip install -r requirements.txt' "
            "ou use --csv-dir apontando para os CSVs baixados manualmente.") from exc
    print(f"[captura] baixando {KAGGLE_DATASET} do Kaggle...")
    try:
        return Path(kagglehub.dataset_download(KAGGLE_DATASET))
    except Exception as exc:
        raise RuntimeError(
            "Falha ao baixar do Kaggle. Baixe o ZIP manualmente em "
            f"https://www.kaggle.com/datasets/{KAGGLE_DATASET}, descompacte e rode "
            "novamente com --csv-dir <pasta dos CSVs>.\n"
            f"Erro original: {exc}") from exc


def _sha256(caminho: Path) -> str:
    h = hashlib.sha256()
    with caminho.open("rb") as f:
        for bloco in iter(lambda: f.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


def _contar_linhas(caminho: Path) -> int:
    with caminho.open("rb") as f:
        return sum(1 for _ in f)


def capturar(cfg: Config, data_carga: str, csv_dir: str | Path | None = None) -> Path:
    """Copia os nove CSVs para a zona bruta e grava o manifesto da carga."""
    origem = Path(csv_dir).expanduser().resolve() if csv_dir else _baixar_do_kaggle()
    faltando = [a for a in ARQUIVOS if not (origem / f"{a}.csv").exists()]
    if faltando:
        raise FileNotFoundError(
            f"Arquivos ausentes em {origem}: {', '.join(f + '.csv' for f in faltando)}")

    destino = cfg.raw_da_carga(data_carga)
    destino.mkdir(parents=True, exist_ok=True)

    manifesto = {
        "dataset": KAGGLE_DATASET,
        "origem": str(origem),
        "data_carga": data_carga,
        "capturado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "arquivos": [],
    }
    for nome in ARQUIVOS:
        src = origem / f"{nome}.csv"
        dst = destino / f"{nome}.csv"
        shutil.copyfile(src, dst)
        manifesto["arquivos"].append({
            "arquivo": dst.name,
            "bytes": dst.stat().st_size,
            "linhas": _contar_linhas(dst) - 1,  # sem o cabeçalho
            "sha256": _sha256(dst),
        })
        print(f"[captura] {dst.name:<18} {manifesto['arquivos'][-1]['linhas']:>9,} linhas"
              .replace(",", "."))

    (destino / "_manifesto.json").write_text(
        json.dumps(manifesto, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[captura] zona bruta: {destino}")
    return destino
