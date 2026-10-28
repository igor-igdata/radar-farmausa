"""
REVISÃO GERAL - limpeza do banco dos radares com as regras da v3 (rodar 1x)
==========================================================================
1. Salva uma cópia das tabelas do radar em JSON na pasta indicada.
2. Apaga os itens gravados que não passam nas regras novas (exame toxicológico,
   "Evr" no meio de "Mionevrix", "Trabec" em trabeculectomia etc.) e preenche
   o total dos itens (quantidade × valor unitário), que a v2 nunca gravou.
3. Para cada edital que ficou sem item do radar: relê todos os itens no PNCP;
   grava os do radar, ou remove o edital se o PNCP tem itens e nenhum é do radar.
4. Revisa as datas dos editais abertos e lê os documentos deles (sem limite).
5. Com --incluir-abertos, grava também os editais abertos que estavam fora do
   radar. Nada disso manda Telegram.

Uso: python REVISAO_GERAL.py <pasta_da_copia> cannabis|altocusto [--incluir-abertos] [--so-limpeza]
     --so-limpeza: faz só os passos 1 a 3 (cópia, itens e editais sem item)
     RADAR_SIMULAR=1 python REVISAO_GERAL.py ...   (só mostra o que faria)
"""

import datetime
import importlib.machinery
import importlib.util
import json
import logging
import os
import sys
import time

import radar_comum as rc

log = logging.getLogger("radar")
SCRIPTS = {"cannabis": "LICITACAO.PY", "altocusto": "LICITACAO_ALTOCUSTO.py"}


def carregar_radar(nome):
    # LICITACAO.PY tem extensão maiúscula: o import comum não acha.
    caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), SCRIPTS[nome])
    loader = importlib.machinery.SourceFileLoader(f"radar_{nome}", caminho)
    mod = importlib.util.module_from_spec(importlib.util.spec_from_loader(loader.name, loader))
    loader.exec_module(mod)
    radar = mod.RADAR
    radar.chat_id = ""  # revisão em massa não avisa ninguém
    return radar


def copiar(radar, pasta):
    os.makedirs(pasta, exist_ok=True)
    carimbo = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    for tabela, ordem in ((radar.tabela_editais, "url_id"), (radar.tabela_itens, "id")):
        linhas = rc.sb_ler(radar, tabela, "*", ordem)
        if linhas is None:
            raise RuntimeError(f"não consegui copiar {tabela}")
        destino = os.path.join(pasta, f"{carimbo}-{tabela}.json")
        with open(destino, "w", encoding="utf-8") as f:
            json.dump(linhas, f, ensure_ascii=False)
        log.info(f"💾 Cópia de {tabela}: {len(linhas)} linha(s) em {destino}")


def limpar_itens(radar):
    itens = rc.sb_ler(radar, radar.tabela_itens, "id,edital_url_id,descricao", "id") or []
    ruins = [i for i in itens if not radar.item_do_radar(i.get("descricao"))]
    log.info(f"🧹 Itens que não passam nas regras novas: {len(ruins)} de {len(itens)}")
    for i in ruins:
        log.info(f"   - {i['edital_url_id']} | {(i.get('descricao') or '')[:90]}")
    for n in range(0, len(ruins), 100):
        ids = ",".join(str(i["id"]) for i in ruins[n:n + 100])
        rc.sb_gravar(radar, "DELETE", radar.tabela_itens, params={"id": f"in.({ids})"},
                     descricao=f"{len(ruins[n:n + 100])} item(ns)")


def completar_totais(radar):
    """Até a v2 o total do item nunca era gravado (o robô lia um campo que não
    existe na API). Total = quantidade × valor unitário, gravado em lotes."""
    itens = rc.sb_ler(radar, radar.tabela_itens, "*", "id") or []
    faltam = []
    for i in itens:
        q, vu = rc._float(i.get("quantidade")), rc._float(i.get("valor_unitario"))
        if i.get("valor_total") is None and q and vu:
            faltam.append({**i, "valor_total": round(q * vu, 2)})
    log.info(f"🧮 Itens sem total que dá para calcular: {len(faltam)} de {len(itens)}")
    for n in range(0, len(faltam), 500):
        lote = faltam[n:n + 500]
        rc.sb_gravar(radar, "POST", radar.tabela_itens, params={"on_conflict": "id"}, json=lote,
                     descricao=f"total de {len(lote)} item(ns)", prefer="resolution=merge-duplicates,return=minimal")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    if len(sys.argv) < 3 or sys.argv[2] not in SCRIPTS:
        print(__doc__)
        sys.exit(2)
    pasta, nome = sys.argv[1], sys.argv[2]
    radar = carregar_radar(nome)
    log.info(f"===== REVISÃO GERAL: {nome}" + ("  [SIMULAÇÃO]" if rc.SIMULAR else ""))
    rc.verificar_colunas(radar)

    copiar(radar, pasta)
    limpar_itens(radar)
    completar_totais(radar)

    editais = rc.sb_ler(radar, radar.tabela_editais,
                        "url_id,objeto,valor_total_estimado,data_fim,data_inicio,link_sistema_origem", "url_id") or []
    vetados = [e for e in editais if radar.objeto_vetado(e.get("objeto"))]
    log.info(f"🚫 Editais com objeto de evento ou exame de laboratório: {len(vetados)}")
    for e in vetados:
        rc.remover_edital(radar, e["url_id"], (e.get("objeto") or "")[:80])
    editais = [e for e in editais if not radar.objeto_vetado(e.get("objeto"))]

    com_itens, sem_detalhe = rc.url_ids_com_itens(radar)
    sem_itens = [e for e in editais if e["url_id"] not in com_itens]
    log.info(f"🔎 Editais sem item do radar: {len(sem_itens)} de {len(editais)}")
    resultado = {"gravou": 0, "removido": 0, None: 0}
    for n, e in enumerate(sem_itens, 1):
        cnpj, ano, seq = rc.partes_url_id(e["url_id"])
        r = rc._conferir_itens(radar, e, cnpj, ano, seq)
        resultado[r] += 1
        if r == "gravou":
            com_itens.add(e["url_id"])
        if n % 20 == 0:
            log.info(f"   … {n}/{len(sem_itens)}")
    log.info(f"   itens gravados: {resultado['gravou']} | removidos: {resultado['removido']} | "
             f"sem mudança: {resultado[None]}")

    extras = []
    if "--incluir-abertos" in sys.argv:
        conhecidos = {e["url_id"] for e in rc.sb_ler(radar, radar.tabela_editais, "url_id", "url_id") or []}
        abertos = rc.buscar_abertos(radar.termos_abertos)
        fora = [c for u, c in abertos.items() if u not in conhecidos]
        log.info(f"📥 Abertos no PNCP: {len(abertos)} | fora do radar: {len(fora)}")
        incluidos = 0
        for c in fora:
            if rc.processar_novo(radar, c):
                incluidos += 1
                com_itens.add(c["url_id"])
        log.info(f"   incluídos: {incluidos}")
        extras = [u for u in abertos if u in conhecidos]

    if "--so-limpeza" in sys.argv:   # sem reler datas e documentos dos abertos
        return
    rc.revisar(radar, rc.editais_para_revisar(radar, extras), com_itens, sem_detalhe,
               alertar=False, ler_documentos=True, limite_pdfs=10**6)


if __name__ == "__main__":
    inicio = time.time()
    main()
    log.info(f"⏱ {time.time() - inicio:.0f}s")
