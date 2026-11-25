"""
PREÇOS ALTO CUSTO - preços pagos nos últimos 12 meses (PNCP)
============================================================
Pedido do Bruno Fortini (e-mail de 06/11/2026): no detalhe do edital e na cotação, quanto os
órgãos pagaram por cada medicamento da lista do alto custo no último ano.

Para cada medicamento de LICITACAO_ALTOCUSTO.MEDICAMENTOS:
1. busca no PNCP as compras que citam o medicamento (todas as situações, 12 meses);
2. em cada compra nova ou atualizada no PNCP, lê os itens e fica com os do medicamento
   (mesmas exclusões do radar: cosmético, Solu-Cortef, teste de laboratório...);
3. item com resultado: lê os resultados homologados (preço unitário pago, quantidade,
   fornecedor); item sem resultado (deserto, fracassado, cancelado, em andamento) também
   entra, sem preço;
4. grava em precos_altocusto (uma linha por resultado) e marca a compra em precos_compras,
   para a noite seguinte só refazer o que mudou.

Grava pelo painel (SUPABASE_URL = .../radar-licitacoes/robo, ver radar_comum._painel). Roda de
madrugada (precos-altocusto.yml), longe das rodadas do horário comercial: a API do PNCP recusa
quem consulta demais.

Uso: python PRECOS_ALTOCUSTO.py [--max-compras N] [--so "Cisteamina"]
     RADAR_SIMULAR=1 python ...   lê o PNCP, mas não grava
"""

import datetime
import logging
import re
import sys
import time
import unicodedata

import radar_comum as rc
import LICITACAO_ALTOCUSTO as AC
from termos import normalizar

log = logging.getLogger("radar")
DIAS = 365
TABELA = "precos_altocusto"
TABELA_COMPRAS = "precos_compras"
PAUSA = 0.8   # segundos entre consultas de itens/resultados
PAUSA_BUSCA = 3.0   # entre páginas e consultas da busca (é ela que recusa quem consulta demais)
ESPERAS_REPETICAO = (90, 240)   # segundos antes da 2ª e da 3ª passada dos medicamentos cuja busca falhou


# ---------------------------------------------------------------- apresentação
# Igual a precos_apresentacao() e precos_forma() em radar-licitacoes-app/lib/precos.php (o painel
# compara o item do edital com o histórico pelas mesmas chaves).
_DOSE = re.compile(r"(\d+(?:[.,]\d+)*)\s*(mg/ml|mcg/ml|ui/ml|mg/g|mcg|mg|ui|%)(?![a-z])")
_CONCENTRACAO = re.compile(r"concentra\w*\s*:?\s*(\d+(?:[.,]\d+)?)")


def forma(descricao, unidade=""):
    """Forma de venda pela descrição e pela unidade (o campo de unidade do PNCP não é confiável)."""
    t = normalizar(f"{descricao or ''} {unidade or ''}")
    u = normalizar(unidade or "")
    if re.search(r" (oftalm|colir|ocular)", t):
        return "oftálmico"
    if re.search(r" (caixa|cx) ", u):
        return "caixa"
    if re.search(r" (capsul|caps |comprimid|cp |cpr |com |drag|tablet)", t):
        return "cápsula/comprimido"
    if re.search(r" (frasco|fr |fco |ampol|serin|canet|ml )", t):
        return "frasco/ampola"
    if re.search(r" (adesiv|patch|transderm)", t):
        return "adesivo"
    return "unidade"


def _numero(num):
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", num):
        num = num.replace(".", "")
    num = num.replace(".", ",")
    if "," in num:
        num = num.rstrip("0").rstrip(",")
    return num


def apresentacao(descricao, unidade=""):
    t = (descricao or "").lower().replace("µg", "mcg").replace("μg", "mcg")
    t = re.sub(r"miligramas?", "mg", t)
    t = re.sub(r"microgramas?", "mcg", t)
    t = re.sub(r"unidades? internacion\w*", "ui", t)
    p = t.find("equiv")
    for x in ([t[p:], t] if p >= 0 else [t]):
        m = _DOSE.search(x)
        if m:
            num, un = _numero(m.group(1)), m.group(2)
            return num + "%" if un == "%" else f"{num} {un}"
    # "concentração: 150" sem unidade (catálogo federal): em cápsula/comprimido é mg
    m = _CONCENTRACAO.search(t)
    if m and forma(descricao, unidade) == "cápsula/comprimido":
        return f"{_numero(m.group(1))} mg"
    return ""


# ---------------------------------------------------------------- PNCP
def compras_do_medicamento(termos, desde):
    """Compras (de qualquer situação) que citam algum dos termos, publicadas de `desde` em diante."""
    achadas = {}
    falhou = False
    for q in rc._consultas([t for t in termos if len(t) > 4 and t not in AC.GENERICOS]):
        for pagina in range(1, 21):
            j = rc._pagina_busca(q, pagina, status="todos")
            if j is None:
                falhou = True
                break
            itens = j["items"]
            for it in itens:
                c = rc.candidato_da_busca(it)
                if c and (c["data_publicacao"] or "")[:10] >= desde:
                    c["municipio"] = it.get("municipio_nome") or ""
                    c["atualizacao"] = it.get("data_atualizacao_pncp") or ""
                    c["tem_resultado"] = bool(it.get("tem_resultado"))
                    achadas.setdefault(c["url_id"], c)
            # a busca ordena pela atualização: página que já passou da data não tem mais nada do período
            if len(itens) < 100 or (itens[-1].get("data_atualizacao_pncp") or "")[:10] < desde:
                break
            time.sleep(PAUSA_BUSCA)
        time.sleep(PAUSA_BUSCA)
    return achadas, falhou


def resultados_do_item(cnpj, ano, seq, numero):
    r = rc._get_pncp(f"{rc.PNCP}/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{numero}/resultados")
    if r is None:
        return None
    if r.status_code in (204, 404):
        return []
    if r.status_code != 200:
        return None
    j = rc._json(r)
    return j if isinstance(j, list) else []


def linhas_da_compra(c, grupos):
    """Linhas de precos_altocusto da compra; None se o PNCP não respondeu (tenta na próxima noite)."""
    itens = rc.buscar_itens(c["cnpj"], c["ano"], c["seq"])
    time.sleep(PAUSA)
    if itens is None:
        return None
    agora = datetime.datetime.now(datetime.timezone.utc).isoformat()
    linhas = []
    for it in itens:
        desc = rc.descricao_item(it)
        if AC.EXCLUI_ITEM.casa(desc):
            continue
        grupo = next((g for g, termos in grupos.items() if termos.casa(desc)), None)
        if not grupo:
            continue
        n = it.get("numeroItem")
        base = {
            "url_id": c["url_id"], "numero_item": str(n), "grupo": grupo,
            "apresentacao": apresentacao(desc, it.get("unidadeMedida")),
            "descricao": desc[:500], "unidade": (it.get("unidadeMedida") or "")[:60] or None,
            "quantidade": rc._float(it.get("quantidade")), "valor_estimado": rc._float(it.get("valorUnitarioEstimado")),
            "situacao_item": it.get("situacaoCompraItemNome"), "orgao": c["orgao"], "uf": c["uf"],
            "municipio": c.get("municipio") or None, "modalidade": c["modalidade"],
            "data_publicacao": c["data_publicacao"], "coletado_em": agora,
            "sequencial": 0, "valor_unitario": None, "quantidade_homologada": None, "valor_total": None,
            "fornecedor": None, "fornecedor_ni": None, "porte_fornecedor": None, "data_resultado": None,
            "situacao_resultado": None,
        }
        res = []
        if it.get("temResultado"):
            res = resultados_do_item(c["cnpj"], c["ano"], c["seq"], n)
            time.sleep(PAUSA)
            if res is None:
                return None
        if not res:
            linhas.append({**base, "chave": f"{c['url_id']}#{n}#0"})
            continue
        for r in res:
            s = r.get("sequencialResultado") or 1
            linhas.append({**base, "chave": f"{c['url_id']}#{n}#{s}", "sequencial": int(s),
                           "valor_unitario": rc._float(r.get("valorUnitarioHomologado")),
                           "quantidade_homologada": rc._float(r.get("quantidadeHomologada")),
                           "valor_total": rc._float(r.get("valorTotalHomologado")),
                           "fornecedor": (r.get("nomeRazaoSocialFornecedor") or "")[:200] or None,
                           "fornecedor_ni": r.get("niFornecedor"), "porte_fornecedor": r.get("porteFornecedorNome"),
                           "data_resultado": (r.get("dataResultado") or "")[:19] or None,
                           "situacao_resultado": r.get("situacaoCompraItemResultadoNome")})
    return linhas


# ---------------------------------------------------------------- execução
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    args = sys.argv[1:]
    max_compras = int(args[args.index("--max-compras") + 1]) if "--max-compras" in args else 900
    so = args[args.index("--so") + 1] if "--so" in args else None
    radar = AC.RADAR
    if "supabase.co" in (radar.supabase_url or ""):
        sys.exit("SUPABASE_URL ainda aponta para o Supabase: o histórico de preços só existe no banco do painel")
    inicio = time.time()
    desde = (rc.agora_brasilia() - datetime.timedelta(days=DIAS)).date().isoformat()
    grupos = {g: AC.termos_do_grupo(g) for g in AC.MEDICAMENTOS if not so or g == so}
    log.info(f"💰 Preços do alto custo desde {desde}: {len(grupos)} medicamento(s)")

    feitas = {l["url_id"]: l for l in (rc.sb_ler(radar, TABELA_COMPRAS, "url_id,data_atualizacao", "url_id") or [])}
    compras = {}
    pendentes = list(grupos)
    # a busca do PNCP recusa conexão quando há consulta demais: o medicamento que falhar volta para a
    # fila e é buscado de novo depois de uma espera (2ª e 3ª passada)
    for passada, espera in enumerate((0,) + ESPERAS_REPETICAO, 1):
        if not pendentes:
            break
        if espera:
            log.info(f"🔁 {passada}ª passada: {len(pendentes)} medicamento(s) com busca incompleta, aguardando {espera}s")
            time.sleep(espera)
        falharam = []
        for g in pendentes:
            achadas, falhou = compras_do_medicamento(grupos[g].termos, desde)
            if falhou:
                falharam.append(g)
            for u, c in achadas.items():
                compras.setdefault(u, {**c, "grupos": set()})["grupos"].add(g)
            log.info(f"   {g}: {len(achadas)} compra(s){' (busca incompleta)' if falhou else ''}")
        pendentes = falharam
    buscas_falhas = len(pendentes)
    if pendentes:
        log.warning(f"⚠️ Busca incompleta depois de 3 passadas: {', '.join(pendentes)} (ficam para a próxima noite)")

    def mudou(c):
        f = feitas.get(c["url_id"])
        return not f or (c["atualizacao"] or "")[:19] > (f.get("data_atualizacao") or "")[:19]

    fila = sorted((c for c in compras.values() if mudou(c)), key=lambda c: c["data_publicacao"] or "", reverse=True)
    log.info(f"📋 {len(compras)} compra(s) no período, {len(fila)} nova(s) ou atualizada(s); esta execução faz até {max_compras}")
    gravadas = sem_resposta = precos = 0
    for c in fila[:max_compras]:
        linhas = linhas_da_compra(c, {g: grupos[g] for g in c["grupos"]})
        if linhas is None:
            sem_resposta += 1
            continue
        precos += sum(1 for l in linhas if l["valor_unitario"])
        # refaz a compra inteira: apaga as linhas antigas e grava as novas
        if not rc.sb_gravar(radar, "DELETE", TABELA, params={"url_id": f"eq.{c['url_id']}"}, descricao=c["url_id"]):
            continue
        if linhas and not rc.sb_gravar(radar, "POST", TABELA, json=linhas, descricao=f"{len(linhas)} linha(s) de {c['url_id']}",
                                       prefer="resolution=merge-duplicates,return=minimal"):
            continue
        rc.sb_gravar(radar, "POST", TABELA_COMPRAS, params={"on_conflict": "url_id"},
                     json={"url_id": c["url_id"], "grupos": ", ".join(sorted(c["grupos"])),
                           "data_atualizacao": (c["atualizacao"] or None) and c["atualizacao"][:19],
                           "data_publicacao": c["data_publicacao"], "orgao": c["orgao"], "uf": c["uf"],
                           "itens": len(linhas), "processado_em": datetime.datetime.now(datetime.timezone.utc).isoformat()},
                     descricao=c["url_id"], prefer="resolution=merge-duplicates,return=minimal")
        gravadas += 1
        if gravadas % 50 == 0:
            log.info(f"   … {gravadas} compra(s) gravada(s), {precos} preço(s) pago(s)")
    log.info("=" * 60)
    log.info(f"💰 Compras gravadas: {gravadas} | preços pagos: {precos} | sem resposta do PNCP: {sem_resposta} | "
             f"buscas incompletas: {buscas_falhas} | faltam: {max(0, len(fila) - max_compras)} | {time.time() - inicio:.0f} s")


if __name__ == "__main__":
    main()
