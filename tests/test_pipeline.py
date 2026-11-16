"""Teste ponta a ponta com um dataset mínimo e um MongoDB em memória (mongomock).

Rodar com:  pytest -q
"""
from pathlib import Path

import mongomock
import pytest

from f1pipeline.agregados import montar_agregados
from f1pipeline.captura import capturar
from f1pipeline.carga import carregar
from f1pipeline.config import Config
from f1pipeline.limpeza import limpar
from f1pipeline.spark import criar_spark

CSVS = {
    "circuits": """circuitId,circuitRef,name,location,country,lat,lng,alt,url
1,interlagos,"Autódromo José Carlos Pace","São Paulo","Brazil",-23.7036,-46.6997,785,http://x
""",
    "status": """statusId,status
1,"Finished"
4,"Collision"
""",
    "drivers": """driverId,driverRef,number,code,forename,surname,dob,nationality,url
1,"massa",19,"MAS","Felipe","Massa","1981-04-25","Brazilian",http://x
2,"alonso",14,"ALO","Fernando","Alonso","1981-07-29","Spanish",http://x
3,"antigo",\\N,\\N,"Piloto","Antigo","1920-01-01","Italian",http://x
""",
    "constructors": """constructorId,constructorRef,name,nationality,url
1,"ferrari","Ferrari","Italian",http://x
2,"renault","Renault","French",http://x
""",
    # A corrida 3 tem data inválida e deve ir para a quarentena.
    "races": """raceId,year,round,circuitId,name,date,time,url
1,2008,18,1,"Brazilian Grand Prix","2008-11-02","17:00:00",http://x
2,2009,16,1,"Brazilian Grand Prix","2009-10-18",\\N,http://x
3,2010,18,1,"Brazilian Grand Prix","data-errada","16:00:00",http://x
""",
    # O resultado 4 aponta para um piloto que não existe (chave órfã).
    "results": """resultId,raceId,driverId,constructorId,number,grid,position,positionText,positionOrder,points,laps,time,milliseconds,fastestLap,rank,fastestLapTime,fastestLapSpeed,statusId
1,1,1,1,2,1,1,1,1,10,71,1:34:11.435,5651435,36,1,1:13.736,210.4,1
2,1,2,2,5,6,2,2,2,8,71,+13.298,5664733,70,3,1:14.229,209.0,1
3,2,2,2,7,10,\\N,R,20,0,0,\\N,\\N,\\N,\\N,\\N,\\N,4
4,1,99,1,9,9,3,3,3,6,71,\\N,\\N,\\N,\\N,\\N,\\N,1
""",
    "qualifying": """qualifyId,raceId,driverId,constructorId,number,position,q1,q2,q3
1,1,1,1,2,1,1:11.830,1:11.875,1:12.368
2,1,2,2,5,6,1:12.214,1:11.966,1:12.967
""",
    "pit_stops": """raceId,driverId,stop,lap,time,duration,milliseconds
1,1,2,38,17:50:00,24.100,24100
1,1,1,9,17:12:00,23.500,23500
""",
    # A volta 2 do piloto 1 está repetida (duplicata).
    "lap_times": """raceId,driverId,lap,position,time,milliseconds
1,1,1,1,1:40.707,100707
1,1,2,1,1:14.000,74000
1,1,2,1,1:14.000,74000
1,2,1,2,1:42.000,102000
""",
}


@pytest.fixture(scope="module")
def spark():
    sessao = criar_spark("f1-testes")
    yield sessao
    sessao.stop()


@pytest.fixture(scope="module")
def execucao(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("f1")
    origem = base / "origem"
    origem.mkdir()
    for nome, conteudo in CSVS.items():
        (origem / f"{nome}.csv").write_text(conteudo, encoding="utf-8")

    cfg = Config(base / "data")
    capturar(cfg, "2026-01-01", origem)
    relatorio = limpar(spark, cfg, "2026-01-01")
    contagens = montar_agregados(spark, cfg)
    db = mongomock.MongoClient()["f1_teste"]
    carregar(spark, cfg, db)
    return cfg, relatorio, contagens, db


def _rel(relatorio, arquivo):
    return next(a for a in relatorio["arquivos"] if a["arquivo"] == arquivo)


def test_zona_bruta_preserva_os_csvs(execucao):
    cfg, *_ = execucao
    raw = cfg.raw_da_carga("2026-01-01")
    assert (raw / "results.csv").read_text(encoding="utf-8") == CSVS["results"]
    assert (raw / "_manifesto.json").exists()


def test_limpeza_rejeita_com_motivo(execucao):
    _, relatorio, _, _ = execucao
    assert _rel(relatorio, "races.csv")["motivos"] == {"tipo_invalido:date": 1}
    assert _rel(relatorio, "results.csv")["motivos"] == {"chave_orfa:driverId": 1}
    assert _rel(relatorio, "lap_times.csv")["motivos"] == {"duplicado": 1}
    assert _rel(relatorio, "drivers.csv")["rejeitados"] == 0


def test_quarentena_e_parquet_gravados(execucao):
    cfg, *_ = execucao
    assert any((cfg.quarantine / "2026-01-01" / "races").glob("*.csv"))
    assert any((cfg.trusted / "results").glob("*.parquet"))
    assert (cfg.trusted / "lap_times" / "year=2008").is_dir()  # particionado por ano


def test_contagem_de_documentos(execucao):
    _, _, contagens, db = execucao
    assert contagens == {"corridas": 2, "pilotos": 3, "equipes": 2, "voltas": 2}
    assert db.corridas.count_documents({}) == 2
    assert db.voltas.count_documents({}) == 2


def test_corrida_e_um_agregado_completo(execucao):
    *_, db = execucao
    corrida = db.corridas.find_one({"_id": 1})
    assert corrida["circuito"]["cidade"] == "São Paulo"
    assert corrida["data"].year == 2008 and corrida["data"].hour == 17
    assert [r["piloto"]["nome"] for r in corrida["resultados"]] == ["Felipe Massa",
                                                                   "Fernando Alonso"]
    vencedor = corrida["resultados"][0]
    assert vencedor["equipe"] == {"id": 1, "nome": "Ferrari"}
    assert vencedor["status"] == "Finished"
    assert vencedor["classificacao"]["q3"] == "1:12.368"
    assert [p["parada"] for p in vencedor["pitStops"]] == [1, 2]  # em ordem de parada
    assert corrida["resultados"][1]["pitStops"] == []


def test_campos_ausentes_nao_sao_gravados(execucao):
    *_, db = execucao
    abandono = db.corridas.find_one({"_id": 2})["resultados"][0]
    assert "posicao" not in abandono and "classificacao" not in abandono
    assert abandono["status"] == "Collision"
    antigo = db.pilotos.find_one({"_id": 3})
    assert "codigo" not in antigo and "carreira" not in antigo


def test_resumo_de_carreira(execucao):
    *_, db = execucao
    massa = db.pilotos.find_one({"_id": 1})["carreira"]
    assert (massa["corridas"], massa["vitorias"], massa["podios"], massa["pontos"]) == (1, 1, 1, 10.0)
    alonso = db.pilotos.find_one({"_id": 2})["carreira"]
    assert (alonso["corridas"], alonso["vitorias"], alonso["podios"]) == (2, 0, 1)
    assert (alonso["primeiraTemporada"], alonso["ultimaTemporada"]) == (2008, 2009)
    assert db.equipes.find_one({"_id": 1})["historico"]["vitorias"] == 1


def test_voltas_por_piloto_e_corrida(execucao):
    *_, db = execucao
    doc = db.voltas.find_one({"_id": {"corridaId": 1, "pilotoId": 1}})
    assert doc["ano"] == 2008
    assert [(v["n"], v["ms"]) for v in doc["voltas"]] == [(1, 100707), (2, 74000)]


def test_carga_e_idempotente(execucao, spark):
    cfg, _, _, db = execucao
    carregar(spark, cfg, db)  # segunda carga não pode duplicar
    assert db.corridas.count_documents({}) == 2
    assert db.voltas.count_documents({}) == 2
