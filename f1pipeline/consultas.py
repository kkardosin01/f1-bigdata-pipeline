"""Etapa 7: consultas de demonstração sobre as coleções carregadas.

Mostram que cada pergunta do caso de uso é respondida sem junções:
a corrida inteira vem de um documento, a carreira de outro, o ritmo de outro.
"""
from __future__ import annotations


def _ms(ms) -> str:
    """Milissegundos -> m:ss.mmm"""
    if ms is None:
        return "-"
    ms = int(round(ms))
    return f"{ms // 60000}:{(ms % 60000) / 1000:06.3f}"


def _tabela(titulo: str, colunas: list[str], linhas: list[list]) -> None:
    print(f"\n=== {titulo} ===")
    if not linhas:
        print("(sem dados)")
        return
    linhas = [[("-" if v is None else str(v)) for v in linha] for linha in linhas]
    larguras = [max(len(colunas[i]), *(len(l[i]) for l in linhas)) for i in range(len(colunas))]
    print("  ".join(c.ljust(larguras[i]) for i, c in enumerate(colunas)))
    print("  ".join("-" * w for w in larguras))
    for l in linhas:
        print("  ".join(v.ljust(larguras[i]) for i, v in enumerate(l)))


def contagem(db) -> None:
    _tabela("1. Documentos por coleção", ["coleção", "documentos"],
            [[c, db[c].count_documents({})] for c in ("corridas", "pilotos", "equipes", "voltas")])


def resultado_da_corrida(db, ano: int, nome: str):
    """Uma única leitura devolve a corrida com circuito, pódio, grid e pit stops."""
    corrida = db.corridas.find_one({"ano": ano, "nome": nome})
    if not corrida or not corrida.get("resultados"):
        corrida = db.corridas.find_one({"ano": ano, "resultados.0": {"$exists": True}},
                                       sort=[("rodada", -1)])
    if not corrida:
        _tabela(f"2. Resultado da corrida ({nome} {ano})", [], [])
        return None
    c = corrida["circuito"]
    linhas = [[r.get("posicao", r["status"]), r["piloto"]["nome"], r["equipe"]["nome"],
               r.get("grid"), r.get("pontos"), len(r.get("pitStops", [])),
               (r.get("classificacao") or {}).get("q3")]
              for r in corrida["resultados"][:5]]
    _tabela(f"2. {corrida['nome']} {corrida['ano']} — {c['nome']}, {c.get('cidade')} "
            f"(1 documento, 5 primeiros)",
            ["pos", "piloto", "equipe", "grid", "pontos", "pit stops", "Q3"], linhas)
    return corrida


def maiores_vencedores(db, limite: int = 10) -> None:
    cursor = (db.pilotos.find({"carreira.vitorias": {"$gt": 0}})
              .sort("carreira.vitorias", -1).limit(limite))
    _tabela(f"3. Pilotos com mais vitórias (top {limite})",
            ["piloto", "nacionalidade", "corridas", "vitórias", "pódios", "pontos", "temporadas"],
            [[p["nome"], p.get("nacionalidade"), p["carreira"]["corridas"],
              p["carreira"]["vitorias"], p["carreira"]["podios"], p["carreira"]["pontos"],
              f"{p['carreira']['primeiraTemporada']}-{p['carreira']['ultimaTemporada']}"]
             for p in cursor])


def equipes_com_mais_pontos(db, limite: int = 5) -> None:
    cursor = (db.equipes.find({"historico.pontos": {"$gt": 0}})
              .sort("historico.pontos", -1).limit(limite))
    _tabela(f"4. Equipes com mais pontos na história (top {limite})",
            ["equipe", "nacionalidade", "corridas", "vitórias", "pontos"],
            [[e["nome"], e.get("nacionalidade"), e["historico"]["corridas"],
              e["historico"]["vitorias"], e["historico"]["pontos"]] for e in cursor])


def pit_stops_por_equipe(db, ano: int) -> None:
    """Agregação sobre documentos aninhados: média de pit stop por equipe na temporada.
    Paradas acima de 60 s (bandeira vermelha, reparos) ficam fora da média."""
    pipeline = [
        {"$match": {"ano": ano}},
        {"$unwind": "$resultados"},
        {"$unwind": "$resultados.pitStops"},
        {"$match": {"resultados.pitStops.ms": {"$lt": 60000}}},
        {"$group": {"_id": "$resultados.equipe.nome",
                    "paradas": {"$sum": 1},
                    "media": {"$avg": "$resultados.pitStops.ms"},
                    "melhor": {"$min": "$resultados.pitStops.ms"}}},
        {"$sort": {"media": 1}},
        {"$limit": 10},
    ]
    _tabela(f"5. Tempo médio de pit stop por equipe em {ano} (tempo total no pit lane)",
            ["equipe", "paradas", "média (s)", "melhor (s)"],
            [[d["_id"], d["paradas"], f"{d['media'] / 1000:.3f}", f"{d['melhor'] / 1000:.3f}"]
             for d in db.corridas.aggregate(pipeline)])


def ritmo_do_vencedor(db, corrida) -> None:
    """Ritmo volta a volta do vencedor: um documento da coleção voltas."""
    if not corrida or not corrida.get("resultados"):
        _tabela("6. Ritmo do vencedor", [], [])
        return
    vencedor = corrida["resultados"][0]
    doc = db.voltas.find_one({"_id": {"corridaId": corrida["_id"],
                                      "pilotoId": vencedor["piloto"]["id"]}})
    titulo = f"6. Ritmo de {vencedor['piloto']['nome']} — {corrida['nome']} {corrida['ano']}"
    if not doc:
        _tabela(titulo + " (sem tempos de volta para esta corrida)", [], [])
        return
    tempos = [v["ms"] for v in doc["voltas"]]
    melhor = min(doc["voltas"], key=lambda v: v["ms"])
    _tabela(titulo + " (1 documento)",
            ["voltas", "melhor volta", "na volta", "média", "volta mais lenta"],
            [[len(tempos), _ms(melhor["ms"]), melhor["n"], _ms(sum(tempos) / len(tempos)),
              _ms(max(tempos))]])
    _tabela("   primeiras 5 voltas", ["volta", "posição", "tempo"],
            [[v["n"], v.get("posicao"), _ms(v["ms"])] for v in doc["voltas"][:5]])


def causas_de_abandono(db, ano: int) -> None:
    pipeline = [
        {"$match": {"ano": ano}},
        {"$unwind": "$resultados"},
        {"$match": {"resultados.posicao": {"$exists": False}}},
        {"$group": {"_id": "$resultados.status", "ocorrencias": {"$sum": 1}}},
        {"$sort": {"ocorrencias": -1}},
        {"$limit": 8},
    ]
    _tabela(f"7. Principais causas de abandono em {ano}", ["status", "ocorrências"],
            [[d["_id"], d["ocorrencias"]] for d in db.corridas.aggregate(pipeline)])


def executar_consultas(db, ano: int = 2023, corrida_ano: int = 2008,
                       corrida_nome: str = "Brazilian Grand Prix") -> None:
    contagem(db)
    corrida = resultado_da_corrida(db, corrida_ano, corrida_nome)
    maiores_vencedores(db)
    equipes_com_mais_pontos(db)
    pit_stops_por_equipe(db, ano)
    ritmo_do_vencedor(db, corrida)
    causas_de_abandono(db, ano)
    print()
