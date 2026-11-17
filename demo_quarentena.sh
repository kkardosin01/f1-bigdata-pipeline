#!/usr/bin/env bash
# Demonstração da quarentena: copia a última carga da zona bruta para uma carga
# nova, planta quatro erros de propósito e roda só a etapa de limpeza.
#
# Uso (na pasta do projeto, depois de ter rodado o pipeline ao menos uma vez):
#   bash demo_quarentena.sh
set -eu
cd "$(dirname "$0")"

# Java 17/21 do Homebrew, se o do sistema não servir (mesma lógica do executar_demo.sh).
if ! java -version 2>&1 | grep -qE 'version "(17|21)'; then
  for V in 17 21; do
    H="$(brew --prefix openjdk@$V 2>/dev/null)/libexec/openjdk.jdk/Contents/Home"
    if [ -x "$H/bin/java" ]; then export JAVA_HOME="$H"; export PATH="$H/bin:$PATH"; break; fi
  done
fi
[ -f .venv/bin/activate ] && . .venv/bin/activate

RAW=data/raw/f1
ORIGEM="$(ls "$RAW" | grep -v demo | sort | tail -1)"
DEMO="${ORIGEM}-demo-quarentena"
rm -rf "${RAW:?}/$DEMO" "data/quarantine/f1/$DEMO"
cp -R "$RAW/$ORIGEM" "$RAW/$DEMO"

echo "Plantando 4 erros na carga $DEMO:"
# 1. corrida com data inválida
echo '9999,2024,99,1,"GP de Teste","data-invalida","10:00:00",\N' >> "$RAW/$DEMO/races.csv"
echo "  races.csv      -> corrida 9999 com data 'data-invalida'"
# 2. resultado apontando para um piloto que não existe
echo '999999,18,99999,1,22,1,1,1,1,10,58,1:34:50.616,5690616,39,2,1:27.452,218.300,1' >> "$RAW/$DEMO/results.csv"
echo "  results.csv    -> resultado do piloto 99999, que não existe"
# 3. piloto sem nome (campo obrigatório)
echo '99998,"teste",\N,\N,\N,"Sem Nome","2000-01-01","Brazilian",\N' >> "$RAW/$DEMO/drivers.csv"
echo "  drivers.csv    -> piloto 99998 sem primeiro nome"
# 4. volta repetida
sed -n 2p "$RAW/$DEMO/lap_times.csv" >> "$RAW/$DEMO/lap_times.csv"
echo "  lap_times.csv  -> a primeira volta do arquivo, repetida"
echo

python -m f1pipeline --etapa limpeza --data-carga "$DEMO"

echo
echo "Linhas na quarentena:"
for f in data/quarantine/f1/"$DEMO"/*/part-*.csv; do
  echo "--- $(basename "$(dirname "$f")")"
  head -6 "$f"
done
