# Pipeline de Big Data: Fórmula 1 → MongoDB

Avaliação N3 de **Linguagens de Programação Disruptivas I** (UniCesusc, turma ADS41).
Autor: **Pedro Alves da Rosa Cardoso**.

Implementação do pipeline de ingestão especificado na N2. Ele captura o dataset
[Formula 1 World Championship (1950 - 2024)](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020)
do Kaggle, valida e limpa os dados com **Apache Spark**, grava em **Parquet** e carrega no
**MongoDB** em quatro coleções de agregados.

```
 1. Kaggle ──► 2. Zona bruta ──► 3. Spark: limpeza ──► 4. Zona confiável
   (9 CSVs)     data/raw/         tipos, nulos,         data/trusted/
                (CSV original)    chaves, quarentena    (Parquet)
                                                             │
 7. Consultas ◄── 6. MongoDB ◄── 5. Spark: agregação ◄───────┘
   (demonstração)  4 coleções      junções e aninhamento
                                   data/aggregates/
```

## Pré-requisitos

| Item | Versão | Para quê |
| --- | --- | --- |
| Python | 3.9 ou superior | Rodar o pipeline |
| Java (JDK ou JRE) | 17 ou 21 | Exigido pelo Spark 4 |
| MongoDB | 6 ou superior | Banco de destino (via Docker, instalação local ou Atlas) |
| Docker (opcional) | qualquer recente | Subir o MongoDB com um comando |

Conferir: `python3 --version`, `java -version`, `docker --version`.
No macOS, o Java pode ser instalado com `brew install openjdk@17`.

## Como executar

Atalho: `./executar_demo.sh` confere o ambiente, instala as dependências, sobe o MongoDB,
roda o pipeline e os testes, e grava tudo em `teste_log.txt`. O passo a passo manual vem a seguir.

### 1. Clonar e instalar as dependências

```bash
git clone https://github.com/kkardosin01/f1-bigdata-pipeline.git
cd f1-bigdata-pipeline
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Subir o MongoDB

```bash
docker compose up -d
```

Isso sobe o MongoDB em `localhost:27017` e o mongo-express (interface web) em
<http://localhost:8081>.

Sem Docker, com o MongoDB instalado na máquina (ex.: `brew install mongodb-community`):

```bash
mkdir -p ~/mongodb-f1
mongod --dbpath ~/mongodb-f1 --bind_ip 127.0.0.1 --port 27017 --fork --logpath ~/mongodb-f1/mongod.log
```

Para usar o Atlas ou outro servidor, informe a URI:
`export MONGO_URI="mongodb+srv://usuario:senha@cluster.mongodb.net"`.

### 3. Rodar o pipeline completo

```bash
python -m f1pipeline
```

O comando executa as sete etapas em sequência e leva cerca de um minuto. O download do
Kaggle é feito pelo `kagglehub`. Se o Kaggle pedir autenticação ou a rede bloquear, há duas saídas:

- definir `KAGGLE_USERNAME` e `KAGGLE_KEY` (Kaggle > Settings > API > Create New Token); ou
- baixar o ZIP pelo site, descompactar e apontar a pasta:

```bash
python -m f1pipeline --csv-dir ~/Downloads/archive
```

### 4. Rodar etapas isoladas

```bash
python -m f1pipeline --etapa captura      # etapas 1-2: download e zona bruta
python -m f1pipeline --etapa limpeza      # etapas 3-4: Spark + Parquet + quarentena
python -m f1pipeline --etapa agregados    # etapa 5: monta os documentos
python -m f1pipeline --etapa carga        # etapa 6: grava no MongoDB
python -m f1pipeline --etapa consultas    # etapa 7: consultas de demonstração
python -m f1pipeline --help               # todas as opções
```

### Alternativa: tudo dentro do Docker

Não exige Python nem Java na máquina:

```bash
docker compose up -d
docker compose --profile pipeline run --rm pipeline
```

### Sem MongoDB (plano B para a demonstração)

`python -m f1pipeline --mongo-uri mongomock://` roda tudo com um banco em memória
(`pip install -r requirements-dev.txt`). Serve para mostrar o fluxo, mas os dados somem ao
final do comando, então só funciona com todas as etapas juntas.

## Saídas esperadas

A saída completa de uma execução real (versão 24 do dataset, 1950-2024) está em
[`docs/saida_exemplo.txt`](docs/saida_exemplo.txt). Os totais mudam quando o dataset é atualizado.

**Limpeza:** cada arquivo mostra linhas lidas, válidas e rejeitadas com o motivo. A versão atual do
dataset chega limpa (nenhuma rejeição); a quarentena é exercitada pelos testes automatizados.

```
[limpeza] results       lidos=  26759  válidos=  26759  rejeitados=    0
[limpeza] lap_times     lidos= 589081  válidos= 589081  rejeitados=    0
```

**Agregados e carga:** quatro coleções no banco `f1`.

```
[carga] corridas  gravados=  1125  total na coleção=  1125
[carga] pilotos   gravados=   861  total na coleção=   861
[carga] equipes   gravados=   212  total na coleção=   212
[carga] voltas    gravados= 11041  total na coleção= 11041
```

**Consultas:** sete consultas, entre elas a corrida inteira lida de um único documento. O dataset só
registra pit stops a partir de 2011, por isso a corrida de 2008 aparece com zero paradas.

```
=== 2. Brazilian Grand Prix 2008 — Autódromo José Carlos Pace, São Paulo (1 documento, 5 primeiros) ===
pos  piloto            equipe      grid  pontos  pit stops  Q3
---  ----------------  ----------  ----  ------  ---------  --------
1    Felipe Massa      Ferrari     1     10.0    0          1:12.368
2    Fernando Alonso   Renault     6     8.0     0          1:12.967
3    Kimi Räikkönen    Ferrari     3     6.0     0          1:12.825
```

**Arquivos gerados** (pasta `data/`, fora do Git):

| Pasta | Conteúdo |
| --- | --- |
| `data/raw/f1/<data>/` | Os nove CSVs originais e `_manifesto.json` (linhas, bytes, SHA-256) |
| `data/trusted/f1/<entidade>/` | Parquet limpo e tipado; `lap_times` particionado por ano |
| `data/quarantine/f1/<data>/<entidade>/` | Linhas rejeitadas em CSV, com a coluna `motivo` |
| `data/aggregates/f1/<colecao>/` | Documentos aninhados prontos para a carga |
| `data/reports/limpeza_<data>.json` | Relatório da limpeza por arquivo |

## Conferindo no MongoDB

Pelo mongo-express (<http://localhost:8081>), pelo MongoDB Compass ou pelo `mongosh`:

```javascript
use f1
db.corridas.findOne({ ano: 2008, nome: "Brazilian Grand Prix" })
db.pilotos.find({}, { nome: 1, "carreira.vitorias": 1 }).sort({ "carreira.vitorias": -1 }).limit(5)
db.voltas.findOne({ "_id.corridaId": 18, "_id.pilotoId": 1 })
db.corridas.getIndexes()
```

## Modelo de dados no MongoDB

| Coleção | Um documento representa | Embute | Referencia |
| --- | --- | --- | --- |
| `corridas` | Um Grande Prêmio completo | circuito, resultados, classificação, pit stops | pilotos, equipes |
| `pilotos` | Um piloto e o resumo da carreira | `carreira` | nenhuma |
| `equipes` | Uma equipe e o resumo histórico | `historico` | nenhuma |
| `voltas` | As voltas de um piloto em uma corrida | lista de voltas | corridas, pilotos |

Campos sem valor na origem (`\N`) não são gravados: um piloto que abandonou não tem `posicao`,
e corridas sem registro de pit stop ficam com `pitStops` vazio.

Índices criados: `corridas {ano, rodada}`, `corridas {resultados.piloto.id}`,
`corridas {resultados.equipe.id}` e `voltas {_id.pilotoId, ano}`.

## Regras de limpeza

1. `\N` e texto vazio viram nulo; textos perdem os espaços das pontas.
2. Cada coluna é convertida para o tipo certo (inteiro, decimal, data). Valor que não converte: `tipo_invalido:<coluna>`.
3. Campos obrigatórios nulos: `obrigatorio_nulo:<coluna>`.
4. Linhas repetidas pela chave: `duplicado` (a primeira é mantida).
5. Integridade referencial, por exemplo todo `raceId` de `results` existe em `races`: `chave_orfa:<coluna>`.
6. Colunas `url` são descartadas.

As linhas rejeitadas não são apagadas: vão para a quarentena com o motivo.

## Estrutura do projeto

```
f1pipeline/
  config.py      caminhos das zonas, dataset e conexão
  captura.py     etapas 1-2: download do Kaggle e zona bruta
  spark.py       sessão Spark local
  limpeza.py     etapas 3-4: validação, quarentena e Parquet
  agregados.py   etapa 5: montagem das quatro coleções
  carga.py       etapa 6: upsert no MongoDB e índices
  consultas.py   etapa 7: consultas de demonstração
  __main__.py    linha de comando
tests/           teste ponta a ponta com dataset mínimo
docs/            saída de exemplo de uma execução
docker-compose.yml, Dockerfile, requirements.txt
```

## Testes

```bash
pip install -r requirements-dev.txt
pytest -q
```

O teste monta um dataset mínimo com três erros plantados (data inválida, chave órfã e volta
duplicada), roda o pipeline inteiro e confere a quarentena, os documentos e a idempotência da carga.

## Diferenças em relação à especificação da N2

- **Carga no MongoDB:** a N2 recomendava o MongoDB Spark Connector. A implementação usa
  `pymongo` com gravação em lote (`bulk_write` com upsert), porque dispensa o download de JARs
  na hora da execução e o volume cabe com folga. O resultado no banco é o mesmo.
- **Voltas e pit stops:** a cobertura histórica depende da versão do dataset; o pipeline não
  fixa anos e aceita o que vier.

## Tecnologias

Python, Apache Spark 4 (PySpark), Apache Parquet, MongoDB, pymongo, kagglehub, Docker Compose, pytest.
