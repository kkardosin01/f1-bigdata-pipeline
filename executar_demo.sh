#!/usr/bin/env bash
# Prepara o ambiente, sobe o MongoDB, roda o pipeline completo e os testes.
# Tudo o que aparece na tela também é gravado em teste_log.txt.
#
# Uso:  ./executar_demo.sh                    (baixa o dataset do Kaggle)
#       ./executar_demo.sh ~/Downloads/archive  (usa CSVs já baixados)
set -u
cd "$(dirname "$0")"
LOG="teste_log.txt"
exec > >(tee "$LOG") 2>&1

CSV_DIR="${1:-}"
passo() { printf '\n########## %s ##########\n' "$1"; }

passo "1. Ambiente"
date
uname -sm
python3 --version || { echo "ERRO: python3 não encontrado."; exit 1; }

# Java 17 ou 21 (exigido pelo Spark 4). Procura também nas instalações do Homebrew.
versao_java() { "$1" -version 2>&1 | awk -F'"' '/version/ {split($2, v, "."); print v[1]; exit}'; }
JAVA_OK=""
if command -v java >/dev/null 2>&1; then
  V="$(versao_java java)"
  if [ "$V" = "17" ] || [ "$V" = "21" ]; then JAVA_OK="java"; fi
fi
if [ -z "$JAVA_OK" ]; then
  for CAND in \
    "$(/usr/libexec/java_home -v 17 2>/dev/null)" \
    "$(/usr/libexec/java_home -v 21 2>/dev/null)" \
    "$(brew --prefix openjdk@17 2>/dev/null)/libexec/openjdk.jdk/Contents/Home" \
    "$(brew --prefix openjdk@21 2>/dev/null)/libexec/openjdk.jdk/Contents/Home"; do
    if [ -n "$CAND" ] && [ -x "$CAND/bin/java" ]; then
      V="$(versao_java "$CAND/bin/java")"
      if [ "$V" = "17" ] || [ "$V" = "21" ]; then
        export JAVA_HOME="$CAND"; export PATH="$JAVA_HOME/bin:$PATH"; JAVA_OK="$CAND"; break
      fi
    fi
  done
fi
if [ -z "$JAVA_OK" ]; then
  echo "ERRO: Java 17 ou 21 não encontrado (versão atual: $(java -version 2>&1 | head -1))."
  echo "Instale com:  brew install openjdk@17   e rode este script de novo."
  exit 2
fi
java -version 2>&1 | head -1

passo "2. Dependências Python (.venv)"
[ -d .venv ] || python3 -m venv .venv
# shellcheck disable=SC1091
. .venv/bin/activate
python -m pip install -q --upgrade pip
python -m pip install -q -r requirements-dev.txt || { echo "ERRO: falha no pip install."; exit 3; }
python -c "import pyspark, pymongo, kagglehub; print('pyspark', pyspark.__version__, '| pymongo', pymongo.version)"

passo "3. MongoDB"
porta_aberta() { python - <<'EOF'
import socket, sys
s = socket.socket(); s.settimeout(1)
sys.exit(0 if s.connect_ex(("127.0.0.1", 27017)) == 0 else 1)
EOF
}
MONGO="real"
if porta_aberta; then
  echo "MongoDB já está respondendo em localhost:27017."
elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  echo "Subindo MongoDB e mongo-express com Docker..."
  docker compose up -d mongo mongo-express
  for _ in $(seq 1 30); do porta_aberta && break; sleep 1; done
  porta_aberta || { echo "AVISO: MongoDB não respondeu em 30 s."; MONGO="memoria"; }
else
  echo "AVISO: nenhum MongoDB em localhost:27017 e Docker indisponível (abra o Docker Desktop)."
  MONGO="memoria"
fi
URI_ARGS=()
if [ "$MONGO" = "memoria" ]; then
  echo "Usando banco em memória (mongomock) só para validar o fluxo."
  URI_ARGS=(--mongo-uri mongomock://)
fi

passo "4. Pipeline completo"
CSV_ARGS=()
[ -n "$CSV_DIR" ] && CSV_ARGS=(--csv-dir "$CSV_DIR")
python -m f1pipeline ${CSV_ARGS[@]+"${CSV_ARGS[@]}"} ${URI_ARGS[@]+"${URI_ARGS[@]}"}
RC_PIPELINE=$?

passo "5. Testes automatizados"
python -m pytest -q
RC_TESTES=$?

passo "RESULTADO"
echo "mongodb:  $MONGO"
echo "pipeline: $([ $RC_PIPELINE -eq 0 ] && echo OK || echo "FALHOU (código $RC_PIPELINE)")"
echo "testes:   $([ $RC_TESTES -eq 0 ] && echo OK || echo "FALHOU (código $RC_TESTES)")"
echo "log salvo em: $(pwd)/$LOG"
[ $RC_PIPELINE -eq 0 ] && [ $RC_TESTES -eq 0 ]
