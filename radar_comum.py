"""
RADAR FARMAUSA - funções comuns aos radares de licitação do PNCP (v3)
=====================================================================
LICITACAO.PY (cannabis) e LICITACAO_ALTOCUSTO.py só definem o que muda entre
os radares (tabelas, termos, grupo do Telegram) e chamam executar().

O que cada execução faz:
1. Busca no PNCP os editais publicados ontem e hoje (horário de Brasília) e
   TODOS os editais ainda recebendo proposta. Assim uma execução que o GitHub
   atrasou ou pulou não perde edital.
2. Edital novo só entra se algum ITEM do PNCP for do radar (lendo todas as
   páginas de itens). Se o PNCP ainda não publicou os itens, entra pelo objeto,
   desde que não seja inscrição em congresso, curso, exame etc.
3. Revisa os editais abertos que já estão no banco: atualiza as datas
   (prorrogação, remarcação), grava os itens que faltavam e tira o que se
   revelou falso positivo.
4. Lê os documentos dos editais abertos (documentos.py).

RADAR_SIMULAR=1 roda tudo sem gravar no banco e sem mandar Telegram.
"""

import datetime
import logging
import os
import time

import requests

from termos import Trechos, CONTEXTO_EVENTO, CONTEXTO_MEDICAMENTO, normalizar
import documentos

log = logging.getLogger("radar")

PNCP = "https://pncp.gov.br"
PNCP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}
DASHBOARD_URL = "https://farmausa.com/radar-licitacoes/"
BRASILIA = datetime.timezone(datetime.timedelta(hours=-3))  # sem horário de verão desde 2019
SIMULAR = os.environ.get("RADAR_SIMULAR") == "1"

PAUSA_ENTRE_BUSCAS = 1.5     # segundos entre consultas à busca (ela bloqueia quem consulta demais)
ESPERA_SEGUNDA_PASSADA = 30  # segundos antes de repetir as consultas que falharam
TERMOS_POR_BUSCA = 15        # termos juntados com OR numa consulta só
ITENS_POR_PAGINA = 500       # máximo que a API de itens entrega por página
DIAS_SEM_DATA = 30           # edital sem data de encerramento é revisado por 30 dias

RESUMO_EXEC = {"analisadas": 0, "encontradas": 0, "novas": 0, "buscas_com_erro": 0}

BLOQUEIO_OBJETO = Trechos([
    "assistencia tecnica", "manutencao preventiva", "manutencao corretiva",
    "coffee break", "vale transporte", "bilhete digital", "servicos de limpeza",
    "material de escritorio", "combustivel",
])

_sessao = requests.Session()


class Radar:
    """O que muda de um radar para o outro."""

    def __init__(self, *, nome, titulo, tabela_editais, tabela_itens, termos_itens,
                 termos_objeto, termos_busca, termos_abertos, cabecalho_alerta,
                 nome_resumo, emoji, supabase_url, supabase_key, telegram_token,
                 chat_id, exclui_item=None, exclui_objeto=CONTEXTO_EVENTO, veta_objeto=None):
        self.nome = nome
        self.titulo = titulo
        self.tabela_editais = tabela_editais
        self.tabela_itens = tabela_itens
        self.termos_itens = termos_itens
        self.termos_objeto = termos_objeto
        self.termos_busca = termos_busca        # editais publicados ontem e hoje
        self.termos_abertos = termos_abertos    # varredura de tudo que recebe proposta
        self.cabecalho_alerta = cabecalho_alerta
        self.nome_resumo = nome_resumo
        self.emoji = emoji
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        self.exclui_item = exclui_item
        self.exclui_objeto = exclui_objeto   # objeto que não basta sozinho (sem item do radar)
        self.veta_objeto = veta_objeto       # objeto que descarta o edital mesmo com item do radar
        # unidade/benefício nos itens e plataforma no edital: só depois de rodar
        # sql/radar_v3.sql no Supabase (main() confere).
        self.colunas_novas = False

    def item_do_radar(self, descricao):
        if not self.termos_itens.casa(descricao):
            return False
        return not (self.exclui_item and self.exclui_item.casa(descricao))

    def objeto_vetado(self, objeto):
        return bool(self.veta_objeto and self.veta_objeto.casa(objeto)
                    and not CONTEXTO_MEDICAMENTO.casa(objeto))

    def objeto_do_radar(self, objeto):
        if not self.termos_objeto.casa(objeto):
            return False
        return not (self.exclui_objeto and self.exclui_objeto.casa(objeto))


# ================= UTILITÁRIOS =================

def agora_brasilia():
    return datetime.datetime.now(BRASILIA).replace(tzinfo=None)


def _float(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def partes_url_id(url_id):
    """/compras/{cnpj}/{ano}/{seq} -> (cnpj, ano, seq)"""
    p = (url_id or "").strip("/").split("/")
    return (p[1], p[2], p[3]) if len(p) >= 4 and p[0] == "compras" else (None, None, None)


def formatar_data_br(data_iso):
    if not data_iso:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(data_iso).replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(data_iso)


def formatar_prazo(data_fim_iso):
    """As datas do PNCP vêm no horário de Brasília, sem fuso."""
    if not data_fim_iso:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(data_fim_iso).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None
    total_horas = (dt - agora_brasilia()).total_seconds() / 3600
    dias, horas = int(total_horas // 24), int(total_horas % 24)
    if dias < 0:
        return f"Encerrado há {abs(dias)}d"
    elif dias == 0:
        return f"⚠️ HOJE — {horas}h restantes"
    elif dias == 1:
        return f"⚠️ AMANHÃ — {horas}h restantes"
    elif dias <= 3:
        return f"⚠️ {dias}d {horas}h restantes"
    return f"{dias} dias restantes"


def formatar_qtd(qtd):
    if qtd is None:
        return None
    try:
        n = float(qtd)
        if n == int(n):
            return f"{int(n):,}".replace(",", ".")
        return f"{n:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(qtd)


def formatar_valor(val):
    f = _float(val)
    if not f or f <= 0:
        return None
    return f"R$ {f:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def montar_link_pncp(cnpj, ano, seq):
    return f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"


def classificar_modalidade(modalidade_nome):
    if not modalidade_nome:
        return "📋 Licitação"
    mod = str(modalidade_nome).lower()
    if "dispensa" in mod:
        return "⚖️ Dispensa"
    elif "inexigibilidade" in mod:
        return "⚖️ Inexigibilidade"
    elif "pregão" in mod:
        return "📈 Pregão"
    elif "concorrência" in mod or "concorrencia" in mod:
        return "🏗️ Concorrência"
    elif "credenciamento" in mod:
        return "📝 Credenciamento"
    return "📋 Licitação"


# ================= SUPABASE =================

def _sb(radar, metodo, tabela, params=None, json=None, prefer="return=minimal", extra=None):
    headers = {
        "apikey": radar.supabase_key,
        "Authorization": f"Bearer {radar.supabase_key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
        **(extra or {}),
    }
    return _sessao.request(metodo, f"{radar.supabase_url}/rest/v1/{tabela}",
                           params=params, json=json, headers=headers, timeout=30)


def sb_ler(radar, tabela, select, ordem, filtros=None):
    """Todas as linhas (o Supabase entrega no máximo 1000 por vez). None se falhar."""
    linhas, offset = [], 0
    while True:
        params = {"select": select, "order": ordem, "limit": 1000, "offset": offset, **(filtros or {})}
        r = _sb(radar, "GET", tabela, params=params, prefer="")
        if r.status_code != 200:
            log.error(f"Supabase GET {tabela}: {r.status_code} {r.text[:200]}")
            return None
        lote = r.json()
        linhas.extend(lote)
        if len(lote) < 1000:
            return linhas
        offset += 1000


def sb_gravar(radar, metodo, tabela, params=None, json=None, descricao="", prefer="return=minimal"):
    if SIMULAR:
        log.info(f"   [simulação] {metodo} {tabela} {descricao}")
        return True
    try:
        r = _sb(radar, metodo, tabela, params=params, json=json, prefer=prefer)
    except requests.exceptions.RequestException as e:
        log.error(f"Supabase {metodo} {tabela} falhou: {e}")
        return False
    if r.status_code in (200, 201, 204):
        return True
    log.error(f"Supabase {metodo} {tabela} {descricao}: {r.status_code} {r.text[:300]}")
    return False


# ================= API PNCP =================

def _get_pncp(url, params=None, tentativas=4):
    """GET no PNCP com nova tentativa quando a API derruba a conexão, responde
    429/5xx ou 200 vazio. Devolve None se não houve resposta útil."""
    espera = 2
    for tentativa in range(1, tentativas + 1):
        pausa = espera
        try:
            r = _sessao.get(url, params=params, headers=PNCP_HEADERS, timeout=30)
            instavel = r.status_code in (429, 500, 502, 503, 504) or (r.status_code == 200 and not r.content)
            if not instavel:
                return r
            motivo = f"respondeu {r.status_code}" + (" vazio" if r.status_code == 200 else "")
            if r.status_code == 429:  # excesso de consultas: espera mais
                pausa = max(espera * 4, int(r.headers.get("Retry-After") or 0))
        except requests.exceptions.RequestException as e:
            motivo = f"recusou a conexão ({e.__class__.__name__})"
        if tentativa == tentativas:
            log.warning(f"PNCP {motivo} após {tentativas} tentativas: {url}")
            return None
        log.warning(f"PNCP {motivo} (tentativa {tentativa}/{tentativas}), aguardando {pausa}s")
        time.sleep(min(pausa, 90))
        espera *= 2


def _json(r):
    try:
        return r.json()
    except ValueError:
        return None


def buscar_compra(cnpj, ano, seq):
    """Dados da compra: datas, situação, número, link do sistema de origem."""
    r = _get_pncp(f"{PNCP}/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}")
    if r is not None and r.status_code == 200:
        dados = _json(r)
        if isinstance(dados, dict):
            return dados
    return None


def buscar_itens(cnpj, ano, seq):
    """Todos os itens da compra, página a página. [] se o PNCP não tem itens;
    None se não respondeu (aí não dá para concluir nada)."""
    url = f"{PNCP}/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    todos = []
    for pagina in range(1, 41):
        r = _get_pncp(url, {"pagina": pagina, "tamanhoPagina": ITENS_POR_PAGINA})
        if r is None:
            return None
        if r.status_code in (204, 404):
            break
        if r.status_code != 200:
            log.warning(f"Itens API retornou {r.status_code} para {cnpj}/{ano}/{seq}")
            return None
        dados = _json(r)
        lista = dados if isinstance(dados, list) else (dados or {}).get("data") or []
        todos.extend(lista)
        if len(lista) < ITENS_POR_PAGINA:
            break
        time.sleep(0.3)
    return todos


def descricao_item(item):
    return item.get("descricao") or item.get("materialOuServicoNome") or ""


def valor_item(item):
    # O campo da API é valorTotal; até a v2 o robô lia valorTotalEstimado, que
    # não existe, e o total do item ficava sempre vazio no banco.
    total = _float(item.get("valorTotal")) or _float(item.get("valorTotalEstimado"))
    if total:
        return total
    unit, qtd = _float(item.get("valorUnitarioEstimado")), _float(item.get("quantidade"))
    return round(unit * qtd, 2) if unit and qtd else 0.0


def item_para_banco(radar, item, url_id):
    linha = {
        "edital_url_id": url_id,
        "numero_item": str(item.get("numeroItem", "-")),
        "descricao": descricao_item(item)[:500],
        "quantidade": _float(item.get("quantidade")) or 0.0,
        "valor_unitario": _float(item.get("valorUnitarioEstimado")),
        "valor_total": valor_item(item) or None,
    }
    if radar.colunas_novas:
        linha["unidade"] = (item.get("unidadeMedida") or "")[:60] or None
        linha["beneficio"] = item.get("tipoBeneficioNome") or "Não informado"
    return linha


def beneficio_curto(nome):
    n = normalizar(nome)
    if "exclusiva" in n:
        return "Exclusivo ME/EPP"
    if "cota" in n:
        return "Cota reservada ME/EPP"
    if "subcontrata" in n:
        return "Subcontratação ME/EPP"
    return "Ampla concorrência" if nome else ""


# ─── API de busca do PNCP ───────────────────────────────────────────────────
# Ela ordena por data de ATUALIZAÇÃO (não de publicação), aceita "a OR b" e
# trata "dantroleno" e "dantrolene" como a mesma palavra.

def _consultas(termos):
    for i in range(0, len(termos), TERMOS_POR_BUSCA):
        grupo = termos[i:i + TERMOS_POR_BUSCA]
        yield " OR ".join(f'"{t}"' if " " in t else t for t in grupo)


def _pagina_busca(q, pagina, status=None):
    params = {"q": q, "tipos_documento": "edital", "ordenacao": "-data",
              "pagina": pagina, "tam_pagina": 100}
    if status:
        params["status"] = status
    r = _get_pncp(f"{PNCP}/api/search/", params)
    if r is None or r.status_code != 200:
        return None
    j = _json(r)
    return j if isinstance(j, dict) and isinstance(j.get("items"), list) else None


def _rodar_consultas(consultas, coletar):
    """Roda as consultas; as que falharem são repetidas uma vez no fim."""
    falhas = []
    for q in consultas:
        if not coletar(q):
            falhas.append(q)
        time.sleep(PAUSA_ENTRE_BUSCAS)
    if not falhas:
        return
    log.info(f"🔁 Segunda passada: {len(falhas)} consulta(s) falharam, aguardando {ESPERA_SEGUNDA_PASSADA}s")
    time.sleep(ESPERA_SEGUNDA_PASSADA)
    restantes = []
    for q in falhas:
        if not coletar(q):
            restantes.append(q)
        time.sleep(PAUSA_ENTRE_BUSCAS)
    RESUMO_EXEC["buscas_com_erro"] += len(restantes)
    if restantes:
        log.warning(f"   ⚠️ Ainda sem resposta após a segunda passada: {len(restantes)} consulta(s)")
    else:
        log.info(f"   ✅ Segunda passada recuperou as {len(falhas)} consulta(s)")


def candidato_da_busca(item):
    cnpj, ano, seq = item.get("orgao_cnpj"), item.get("ano"), item.get("numero_sequencial")
    if not (cnpj and ano and seq):
        return None
    return {
        "url_id": f"/compras/{cnpj}/{ano}/{seq}",
        "cnpj": str(cnpj), "ano": str(ano), "seq": str(seq),
        "objeto": item.get("description") or item.get("title") or "",
        "orgao": item.get("orgao_nome") or "",
        "uf": item.get("uf") or "",
        "modalidade": item.get("modalidade_licitacao_nome") or "",
        "data_publicacao": item.get("data_publicacao_pncp"),
        "data_inicio": item.get("data_inicio_vigencia"),
        "data_fim": item.get("data_fim_vigencia"),
        "valor": item.get("valor_total_estimado"),
        "numero_controle": item.get("item_url") or "",
    }


def buscar_publicados(termos, desde):
    """Editais publicados de `desde` (date) em diante."""
    alvo = desde.isoformat()
    achados = {}

    def coletar(q):
        for pagina in range(1, 11):
            j = _pagina_busca(q, pagina)
            if j is None:
                return False
            itens = j["items"]
            for it in itens:
                c = candidato_da_busca(it)
                if c and (c["data_publicacao"] or "") >= alvo:
                    achados.setdefault(c["url_id"], c)
            # Ordem por atualização: quando a página já chegou antes do dia alvo,
            # as seguintes não têm nada publicado no período.
            if len(itens) < 100 or (itens[-1].get("data_atualizacao_pncp") or "") < alvo:
                return True
            time.sleep(PAUSA_ENTRE_BUSCAS)
        return True

    _rodar_consultas(list(_consultas(termos)), coletar)
    return achados


def buscar_abertos(termos):
    """Todos os editais que ainda recebem proposta, de qualquer data."""
    achados = {}

    def coletar(q):
        vistos = 0
        for pagina in range(1, 21):
            j = _pagina_busca(q, pagina, status="recebendo_proposta")
            if j is None:
                return False
            itens = j["items"]
            for it in itens:
                c = candidato_da_busca(it)
                if c:
                    achados.setdefault(c["url_id"], c)
            vistos += len(itens)
            if len(itens) < 100 or vistos >= (j.get("total") or 0):
                return True
            time.sleep(PAUSA_ENTRE_BUSCAS)
        return True

    _rodar_consultas(list(_consultas(termos)), coletar)
    return achados


# ================= TELEGRAM =================

def enviar_mensagem(radar, texto):
    if SIMULAR:
        log.info("   [simulação] Telegram: " + texto.split("\n", 1)[0])
        return True
    if not radar.telegram_token or not radar.chat_id:
        return False
    for _ in range(3):
        try:
            resp = _sessao.post(
                f"https://api.telegram.org/bot{radar.telegram_token}/sendMessage",
                json={"chat_id": radar.chat_id, "text": texto, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            log.error(f"Telegram request falhou: {e}")
            return False
        if resp.status_code == 429:
            # Grupo aceita ~20 mensagens por minuto; o Telegram diz quanto esperar.
            espera = ((_json(resp) or {}).get("parameters") or {}).get("retry_after", 10)
            log.warning(f"Telegram pediu para esperar {espera}s")
            time.sleep(min(int(espera), 60) + 1)
            continue
        if resp.status_code == 200:
            time.sleep(3)
            return True
        log.error(f"Telegram erro {resp.status_code}: {resp.text[:200]}")
        return False
    return False


def _rodape(edital):
    cnpj, ano, seq = partes_url_id(edital["url_id"])
    return (f"🔗 <a href=\"{montar_link_pncp(cnpj, ano, seq)}\">Abrir Edital no PNCP</a>\n"
            f"📊 <a href=\"{DASHBOARD_URL}\">Ver no Radar FarmaUSA</a>")


def _esc(texto):
    """O Telegram recusa a mensagem inteira se um "&" ou "<" do texto vier cru."""
    return str(texto or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _cabeca(edital, titulo):
    modalidade_label = classificar_modalidade(edital.get("modalidade"))
    uf = edital.get("uf", "")
    linha_local = f"{modalidade_label}  |  {uf}" if uf else modalidade_label
    return (f"{titulo}\n{linha_local}\n\n"
            f"🏢 {_esc(edital.get('orgao') or 'N/A')}\n"
            f"📋 {_esc(edital.get('titulo') or 'N/A')}")


def fora_do_radar(edital):
    """Pregão presencial: fica no banco, mas fora do painel e dos alertas
    (pedido do time de licitação em 27/10/2026)."""
    return "presencial" in normalizar(edital.get("modalidade"))


def alerta_novo(radar, edital, escolhidos, compra=None):
    if fora_do_radar(edital):
        log.info("   🔕 Pregão presencial: gravado sem alerta")
        return
    compra = compra or {}
    bloco_produtos = ""
    if escolhidos:
        linhas = []
        for it in escolhidos[:5]:
            linhas.append(f"• Item {it.get('numeroItem', '-')} — {_esc(descricao_item(it)[:100])}")
            partes = []
            qtd = formatar_qtd(it.get("quantidade"))
            if qtd:
                partes.append(f"📦 {qtd} {_esc((it.get('unidadeMedida') or 'un.').strip())}")
            unit = formatar_valor(it.get("valorUnitarioEstimado"))
            if unit:
                partes.append(f"💲 {unit}/un.")
            total = formatar_valor(valor_item(it))
            if total:
                partes.append(f"💰 Total: {total}")
            if partes:
                linhas.append("  ↳ " + " | ".join(partes))
        if len(escolhidos) > 5:
            linhas.append(f"• … e mais {len(escolhidos) - 5} item(ns)")
        bloco_produtos = "\n\n💊 <b>PRODUTO(S):</b>\n" + "\n".join(linhas)

    # Exclusividade ME/EPP, item a item, como o PNCP informa.
    bloco_benef = ""
    tipos = {}
    for it in escolhidos:
        tipos.setdefault(beneficio_curto(it.get("tipoBeneficioNome")), []).append(str(it.get("numeroItem", "-")))
    tipos.pop("", None)
    if tipos:
        if len(tipos) == 1:
            bloco_benef = f"\n🏷 {next(iter(tipos))}"
        else:
            bloco_benef = "\n🏷 " + " | ".join(f"{t}: itens {', '.join(n[:8])}" for t, n in tipos.items())

    linhas_prazo = []
    if edital.get("data_inicio"):
        linhas_prazo.append(f"🟢 Propostas a partir de: {formatar_data_br(edital['data_inicio'])}")
    if edital.get("data_fim"):
        prazo = formatar_prazo(edital["data_fim"])
        linhas_prazo.append(f"🔴 Disputa (fim das propostas): <b>{formatar_data_br(edital['data_fim'])}</b>"
                            + (f"  ← {prazo}" if prazo else ""))
    bloco_prazos = "\n\n⏱ <b>PRAZOS:</b>\n" + "\n".join(linhas_prazo) if linhas_prazo else ""

    bloco_plataforma = ""
    plataforma, link = compra.get("usuarioNome") or "", edital.get("link_sistema_origem") or ""
    if plataforma or link:
        acesso = f" — <a href=\"{_esc(link)}\">acessar</a>" if link.startswith("http") else ""
        # É o sistema que publicou no PNCP; quase sempre é onde acontece a disputa.
        bloco_plataforma = f"\n🖥 Sistema de origem: {_esc(plataforma or 'link do processo')}{acesso}"

    # Edital antigo que só entrou agora (varredura dos abertos): avisa a data real.
    nota = ""
    if not publicado_recente(edital):
        nota = f"\n\n📌 Publicado em {formatar_data_br(edital['data_publicacao'][:10])[:10]}; estava fora do radar."

    msg = (_cabeca(edital, radar.cabecalho_alerta.format(tipo="NOVA LICITAÇÃO"))
           + f"\n📝 {_esc((edital.get('objeto') or '')[:200])}"
           + bloco_produtos + bloco_benef + bloco_prazos + bloco_plataforma + nota + "\n\n" + _rodape(edital))
    if enviar_mensagem(radar, msg):
        log.info(f"🚀 Telegram enviado: {edital.get('orgao', '')[:60]}")


def publicado_recente(edital, dias=2):
    pub = (edital.get("data_publicacao") or "")[:10]
    return not pub or pub >= (agora_brasilia().date() - datetime.timedelta(days=dias)).isoformat()


def alerta_atrasados(radar, lista):
    """Editais publicados há dias que só entraram agora pela varredura dos abertos
    (na primeira execução da v3 podem ser dezenas): uma mensagem de resumo em vez
    de um alerta por edital."""
    lista = sorted([x for x in lista if not fora_do_radar(x[0])], key=lambda x: x[0].get("data_fim") or "9999")
    for n in range(0, len(lista), 20):
        linhas = []
        for edital, escolhidos in lista[n:n + 20]:
            cnpj, ano, seq = partes_url_id(edital["url_id"])
            produto = descricao_item(escolhidos[0]) if escolhidos else (edital.get("objeto") or "")
            fim = formatar_data_br(edital.get("data_fim")) or "sem data"
            linhas.append(f"• {_esc(edital.get('uf') or '')} | {_esc((edital.get('orgao') or '')[:45])}\n"
                          f"  {_esc(produto[:70])} — até {fim} · <a href=\"{montar_link_pncp(cnpj, ano, seq)}\">PNCP</a>")
        cab = radar.cabecalho_alerta.format(tipo="ABERTOS QUE ESTAVAM FORA DO RADAR")
        msg = (f"{cab}\n{len(lista)} edital(is) ainda recebendo proposta entraram pela varredura dos abertos"
               + (f" (parte {n // 20 + 1})" if len(lista) > 20 else "") + ":\n\n"
               + "\n".join(linhas) + f"\n\n📊 <a href=\"{DASHBOARD_URL}\">Ver todos no Radar FarmaUSA</a>")
        enviar_mensagem(radar, msg)


def alerta_prazo(radar, edital, fim_antigo, fim_novo):
    if fora_do_radar(edital):
        return
    prazo = formatar_prazo(fim_novo)
    msg = (_cabeca(edital, radar.cabecalho_alerta.format(tipo="PRAZO ALTERADO"))
           + f"\n\n🔴 Encerramento: {formatar_data_br(fim_antigo)} → <b>{formatar_data_br(fim_novo)}</b>"
           + (f"  ← {prazo}" if prazo else "") + "\n\n" + _rodape(edital))
    enviar_mensagem(radar, msg)


def alerta_documento(radar, edital, novos, avisos):
    if fora_do_radar(edital):
        return
    linhas =[f"📄 <a href=\"{d['url']}\">{_esc(d['titulo'])}</a> ({_esc(d['tipo'])})" for d in novos[:6]]
    if len(novos) > 6:
        linhas.append(f"… e mais {len(novos) - 6} documento(s)")
    for a in avisos[:3]:
        linhas.append(f"⚠️ {_esc(a)}")
    msg = (_cabeca(edital, radar.cabecalho_alerta.format(tipo="NOVO DOCUMENTO NO EDITAL"))
           + "\n\n" + "\n".join(linhas) + "\n\n" + _rodape(edital))
    enviar_mensagem(radar, msg)


def alerta_situacao(radar, edital, situacao):
    if fora_do_radar(edital):
        return
    msg =(_cabeca(edital, radar.cabecalho_alerta.format(tipo="EDITAL " + situacao.upper()))
           + "\n\n" + _rodape(edital))
    enviar_mensagem(radar, msg)


# ================= PROCESSAMENTO =================

def montar_edital(radar, c, compra, escolhidos):
    compra = compra or {}
    valor = sum(valor_item(i) for i in escolhidos) or _float(c.get("valor"))
    numero = compra.get("numeroCompra")
    titulo = f"Edital nº {numero}/{compra.get('anoCompra')}" if numero else f"Edital nº {c['seq']}"
    extra = {"plataforma": compra.get("usuarioNome") or None} if radar.colunas_novas else {}
    return {
        **extra,
        "url_id": c["url_id"],
        "titulo": titulo,
        "objeto": (compra.get("objetoCompra") or c.get("objeto") or "")[:500],
        "orgao": (compra.get("orgaoEntidade") or {}).get("razaoSocial") or c.get("orgao") or "",
        "uf": (compra.get("unidadeOrgao") or {}).get("ufSigla") or c.get("uf") or "",
        "modalidade": compra.get("modalidadeNome") or c.get("modalidade") or "",
        "data_publicacao": compra.get("dataPublicacaoPncp") or c.get("data_publicacao"),
        "valor_total_estimado": valor if valor and valor > 0 else None,
        "data_inicio": compra.get("dataAberturaProposta") or c.get("data_inicio"),
        "data_fim": compra.get("dataEncerramentoProposta") or c.get("data_fim"),
        "numero_controle_pncp": c.get("numero_controle") or c["url_id"],
        "link_sistema_origem": compra.get("linkSistemaOrigem") or "",
    }


def processar_novo(radar, c, atrasados=None):
    """Confere os itens do edital no PNCP e, se for do radar, grava e avisa.
    Com a lista `atrasados`, edital publicado há dias vai para ela (resumo
    único no fim) em vez de ganhar um alerta só dele."""
    objeto = c.get("objeto") or ""
    log.info(f"   🎯 {c['uf']} | {c['orgao'][:40]} | {objeto[:70]}")
    if BLOQUEIO_OBJETO.casa(objeto) or radar.objeto_vetado(objeto):
        log.info("   🚫 Objeto bloqueado (evento, exame de laboratório ou outro contexto não farmacêutico)")
        return False

    itens = buscar_itens(c["cnpj"], c["ano"], c["seq"])
    time.sleep(0.3)
    if itens is None:
        log.warning("   ⚠️ PNCP não entregou os itens; fica para a próxima execução")
        return False
    escolhidos = [i for i in itens if radar.item_do_radar(descricao_item(i))]
    if not escolhidos:
        # Item genérico ("LOTE 01") ou ainda não publicado: vale o objeto citar o
        # produto por extenso, fora de contexto de evento/exame.
        if not radar.objeto_do_radar(objeto):
            log.info(f"   ⏭️  {len(itens)} item(ns) no PNCP, nenhum do radar, e o objeto não confirma: ignorado")
            return False
        log.info(f"   ⚠️  Nenhum dos {len(itens)} item(ns) cita o produto, mas o objeto cita: entra")

    compra = buscar_compra(c["cnpj"], c["ano"], c["seq"])
    time.sleep(0.5)
    edital = montar_edital(radar, c, compra, escolhidos)
    if not sb_gravar(radar, "POST", radar.tabela_editais, params={"on_conflict": "url_id"},
                     json=edital, descricao=edital["url_id"],
                     prefer="resolution=merge-duplicates,return=minimal"):
        return False
    log.info(f"✅ Novo no banco: {edital['url_id']} ({len(escolhidos)} item(ns) do radar)")
    if escolhidos:
        sb_gravar(radar, "POST", radar.tabela_itens,
                  json=[item_para_banco(radar, i, edital["url_id"]) for i in escolhidos],
                  descricao=f"{len(escolhidos)} item(ns)")
    if atrasados is not None and not publicado_recente(edital):
        atrasados.append((edital, escolhidos))
    else:
        alerta_novo(radar, edital, escolhidos, compra)
    return True


def remover_edital(radar, url_id, motivo):
    log.info(f"   🗑️  Removido ({motivo}): {url_id}")
    sb_gravar(radar, "DELETE", radar.tabela_itens, params={"edital_url_id": f"eq.{url_id}"}, descricao=url_id)
    sb_gravar(radar, "DELETE", radar.tabela_editais, params={"url_id": f"eq.{url_id}"}, descricao=url_id)


def _conferir_itens(radar, e, cnpj, ano, seq, regravar=False):
    """Relê os itens do edital no PNCP e grava os do radar. Com regravar=True
    troca os itens que já estavam no banco (para trazer unidade e benefício).
    Devolve "gravou", "removido" ou None (nada mudou ou o PNCP não respondeu)."""
    url_id = e["url_id"]
    itens = buscar_itens(cnpj, ano, seq)
    time.sleep(0.5)
    if itens is None:
        return None
    escolhidos = [i for i in itens if radar.item_do_radar(descricao_item(i))]
    if escolhidos:
        if regravar and not sb_gravar(radar, "DELETE", radar.tabela_itens,
                                      params={"edital_url_id": f"eq.{url_id}"}, descricao=url_id):
            return None
        if not sb_gravar(radar, "POST", radar.tabela_itens,
                         json=[item_para_banco(radar, i, url_id) for i in escolhidos],
                         descricao=f"{len(escolhidos)} item(ns) de {url_id}"):
            return None
        log.info(f"   📦 {len(escolhidos)} item(ns) {'regravado' if regravar else 'gravado'}(s): {url_id}")
        valor = sum(valor_item(i) for i in escolhidos)
        if not e.get("valor_total_estimado") and valor > 0:
            sb_gravar(radar, "PATCH", radar.tabela_editais, params={"url_id": f"eq.{url_id}"},
                      json={"valor_total_estimado": valor}, descricao=url_id)
        return "gravou"
    if regravar or radar.objeto_do_radar(e.get("objeto")):
        return None   # item genérico ("LOTE 01") ou ainda sem itens: o objeto sustenta
    if itens:
        remover_edital(radar, url_id, f"{len(itens)} item(ns) no PNCP e nenhum do radar")
        return "removido"
    if radar.exclui_objeto and radar.exclui_objeto.casa(e.get("objeto")):
        remover_edital(radar, url_id, "PNCP sem itens e objeto de evento/inscrição")
        return "removido"
    return None


def _datas_novas(radar, e, compra):
    mudou = {}
    for campo, chave in (("data_inicio", "dataAberturaProposta"), ("data_fim", "dataEncerramentoProposta")):
        novo = compra.get(chave)
        if novo and (e.get(campo) or "")[:16] != novo[:16]:
            mudou[campo] = novo
    if not e.get("link_sistema_origem") and compra.get("linkSistemaOrigem"):
        mudou["link_sistema_origem"] = compra["linkSistemaOrigem"]
    if radar.colunas_novas and not e.get("plataforma") and compra.get("usuarioNome"):
        mudou["plataforma"] = compra["usuarioNome"]
    return mudou


def revisar(radar, editais, com_itens, sem_detalhe=(), alertar=True, ler_documentos=True, limite_pdfs=None):
    """Confere no PNCP os editais que já estão no banco: itens que faltavam,
    falso positivo, datas (prorrogação, remarcação) e documentos.
    sem_detalhe: editais cujos itens foram gravados sem unidade/benefício."""
    contagem = {"datas": 0, "itens": 0, "removidos": 0}
    docs = documentos.Leitor(radar, limite_pdfs or documentos.MAX_PDFS_POR_EXECUCAO) if ler_documentos else None
    for n, e in enumerate(editais, 1):
        url_id = e["url_id"]
        cnpj, ano, seq = partes_url_id(url_id)
        if not cnpj:
            continue
        if radar.objeto_vetado(e.get("objeto")):
            remover_edital(radar, url_id, "objeto de evento ou exame de laboratório")
            contagem["removidos"] += 1
            continue
        if url_id not in com_itens or url_id in sem_detalhe:
            resultado = _conferir_itens(radar, e, cnpj, ano, seq, regravar=url_id in com_itens)
            if resultado == "removido":
                contagem["removidos"] += 1
                continue
            if resultado == "gravou":
                contagem["itens"] += 1
                com_itens.add(url_id)

        compra = buscar_compra(cnpj, ano, seq)
        time.sleep(0.5)
        if not compra:
            continue
        mudou = _datas_novas(radar, e, compra)
        if mudou and sb_gravar(radar, "PATCH", radar.tabela_editais, params={"url_id": f"eq.{url_id}"},
                               json=mudou, descricao=f"{url_id} {list(mudou)}"):
            if "data_fim" in mudou or "data_inicio" in mudou:
                contagem["datas"] += 1
                log.info(f"   📅 Datas atualizadas: {url_id} | fim {e.get('data_fim')} → {mudou.get('data_fim', e.get('data_fim'))}")
            if alertar and e.get("data_fim") and "data_fim" in mudou:
                alerta_prazo(radar, {**e, **mudou}, e["data_fim"], mudou["data_fim"])
            e = {**e, **mudou}
        if docs:
            docs.atualizar(e, compra, alertar=alertar)
        if n % 25 == 0:
            log.info(f"   … {n}/{len(editais)} revisados")
    log.info(f"📋 Revisão: {len(editais)} edital(is) | datas alteradas {contagem['datas']} | "
             f"itens preenchidos {contagem['itens']} | removidos {contagem['removidos']}")
    return contagem


# ================= RESUMO DIÁRIO =================

def enviar_resumo_dia(radar):
    hoje = agora_brasilia().date()
    editais = sb_ler(radar, radar.tabela_editais, "url_id,orgao,uf,modalidade,valor_total_estimado,titulo",
                     "valor_total_estimado.desc.nullslast",
                     {"data_publicacao": f"gte.{hoje.isoformat()}T00:00:00"})
    if editais is None:
        return
    editais = [e for e in editais if not fora_do_radar(e)]
    hoje_fmt = hoje.strftime("%d/%m/%Y")
    if not editais:
        enviar_mensagem(radar, f"📊 <b>Resumo do dia — {hoje_fmt}</b>\n\n"
                               f"Nenhuma licitação {radar.nome_resumo} encontrada hoje.")
        return

    itens = sb_ler(radar, radar.tabela_itens, "edital_url_id,descricao,quantidade,valor_unitario", "id",
                   {"edital_url_id": "in.(" + ",".join(f'"{e["url_id"]}"' for e in editais) + ")"}) or []
    blocos, valor_dia = [], 0.0
    for i, edital in enumerate(editais, 1):
        valor_dia += _float(edital.get("valor_total_estimado")) or 0.0
        cabecalho = f"{i}. {classificar_modalidade(edital.get('modalidade'))}"
        if edital.get("uf"):
            cabecalho += f" | {edital['uf']}"
        linhas = [cabecalho, f"🏢 {edital.get('orgao') or 'N/A'}"]
        for item in [x for x in itens if x["edital_url_id"] == edital["url_id"]][:5]:
            linha = f"   • {_esc((item.get('descricao') or '')[:80])}"
            detalhes = []
            q = _float(item.get("quantidade"))
            if q:
                detalhes.append(f"{int(q):,}".replace(",", ".") + " un.")
            v = formatar_valor(item.get("valor_unitario"))
            if v:
                detalhes.append(v + "/un.")
            if detalhes:
                linha += " — " + " | ".join(detalhes)
            linhas.append(linha)
        blocos.append("\n".join(linhas))

    separador = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    valor_fmt = f"\n💰 Valor total do dia: {formatar_valor(valor_dia)}" if valor_dia > 0 else ""
    msg = (f"📊 <b>Resumo do dia — {hoje_fmt}</b>\n"
           f"{radar.emoji} {len(editais)} licitação(ões) {radar.nome_resumo} encontrada(s)\n"
           f"{separador}{separador.join(blocos)}{separador.rstrip()}"
           f"{valor_fmt}\n"
           f'📊 <a href="{DASHBOARD_URL}">Ver no Radar FarmaUSA</a>')
    if enviar_mensagem(radar, msg):
        log.info(f"📊 Resumo diário enviado — {len(editais)} edital(is).")


# ================= EXECUÇÃO =================

def editais_para_revisar(radar, extras=()):
    """Abertos no banco (encerramento no futuro, ou sem data e publicados há pouco)
    mais os conhecidos que a busca acabou de ver recebendo proposta."""
    agora = agora_brasilia()
    limite = (agora - datetime.timedelta(days=DIAS_SEM_DATA)).date().isoformat()
    sel = "url_id,orgao,uf,modalidade,titulo,objeto,data_publicacao,data_inicio,data_fim,valor_total_estimado,link_sistema_origem"
    if radar.colunas_novas:
        sel += ",plataforma"
    abertos = sb_ler(radar, radar.tabela_editais, sel, "url_id", {
        "or": f"(data_fim.gte.{agora.strftime('%Y-%m-%dT%H:%M')},and(data_fim.is.null,data_publicacao.gte.{limite}))"
    }) or []
    vistos = {e["url_id"] for e in abertos}
    faltam = [u for u in extras if u not in vistos]
    for i in range(0, len(faltam), 50):
        lote = faltam[i:i + 50]
        abertos += sb_ler(radar, radar.tabela_editais, sel, "url_id",
                          {"url_id": "in.(" + ",".join(f'"{u}"' for u in lote) + ")"}) or []
    return abertos


def verificar_colunas(radar):
    """As colunas unidade/beneficio (itens) e plataforma (editais) já existem?"""
    for tabela, cols in ((radar.tabela_itens, "unidade,beneficio"), (radar.tabela_editais, "plataforma")):
        r = _sb(radar, "GET", tabela, params={"select": cols, "limit": 1}, prefer="")
        if r.status_code != 200:
            radar.colunas_novas = False
            log.info("ℹ️  Colunas unidade/benefício/plataforma ainda não criadas (sql/radar_v3.sql)")
            return False
    radar.colunas_novas = True
    return True


def url_ids_com_itens(radar):
    """(editais com item do radar, editais com item gravado sem unidade/benefício)"""
    sel = "edital_url_id,beneficio" if radar.colunas_novas else "edital_url_id"
    linhas = sb_ler(radar, radar.tabela_itens, sel, "id")
    if linhas is None:
        return None, None
    sem_detalhe = {l["edital_url_id"] for l in linhas if radar.colunas_novas and not l.get("beneficio")}
    return {l["edital_url_id"] for l in linhas}, sem_detalhe


def main(radar):
    agora = agora_brasilia()
    log.info("=" * 60)
    log.info(radar.titulo)
    log.info(f"⏰ Execução: {agora.strftime('%d/%m/%Y %H:%M:%S')} (Brasília)" + ("  [SIMULAÇÃO]" if SIMULAR else ""))
    log.info("=" * 60)

    if not radar.supabase_url or not radar.supabase_key:
        log.error("❌ SUPABASE_URL ou SUPABASE_KEY não configurados!")
        raise SystemExit(1)
    if not radar.telegram_token or not radar.chat_id:
        log.warning("⚠️ Telegram não configurado — alertas desativados")

    verificar_colunas(radar)
    conhecidos = sb_ler(radar, radar.tabela_editais, "url_id", "url_id")
    com_itens, sem_detalhe = url_ids_com_itens(radar)
    if conhecidos is None or com_itens is None:
        raise RuntimeError("Não foi possível ler o banco")
    conhecidos = {e["url_id"] for e in conhecidos}

    ontem = agora.date() - datetime.timedelta(days=1)
    log.info(f"🔍 Publicados desde {ontem.strftime('%d/%m/%Y')}: {len(radar.termos_busca)} termos")
    candidatos = buscar_publicados(radar.termos_busca, ontem)
    log.info(f"   {len(candidatos)} edital(is) na busca por data")
    log.info(f"🔍 Recebendo proposta: {len(radar.termos_abertos)} termos")
    abertos = buscar_abertos(radar.termos_abertos)
    log.info(f"   {len(abertos)} edital(is) abertos na busca")
    for url_id, c in abertos.items():
        candidatos.setdefault(url_id, c)

    novos = [c for u, c in candidatos.items() if u not in conhecidos]
    log.info(f"📋 {len(candidatos)} candidato(s): {len(novos)} fora do banco")
    total_novas = 0
    atrasados = []
    for c in novos:
        if processar_novo(radar, c, atrasados):
            total_novas += 1
            conhecidos.add(c["url_id"])
            com_itens.add(c["url_id"])  # já saiu com os itens; a revisão não precisa relê-los
    if atrasados:
        alerta_atrasados(radar, atrasados)

    revisar(radar, editais_para_revisar(radar, [u for u in abertos if u in conhecidos]), com_itens, sem_detalhe)

    RESUMO_EXEC.update(analisadas=len(candidatos), encontradas=len(novos), novas=total_novas)
    log.info("=" * 60)
    log.info("📊 RESUMO:")
    if RESUMO_EXEC["buscas_com_erro"]:
        log.warning(f"   ⚠️ Consultas que falharam mesmo após retry: {RESUMO_EXEC['buscas_com_erro']}")
    log.info(f"   Candidatos:        {len(candidatos)}")
    log.info(f"   Fora do banco:     {len(novos)}")
    log.info(f"   Novos (Telegram):  {total_novas}")
    log.info("=" * 60)

    if agora.hour >= 18:  # 18h de Brasília em diante
        enviar_resumo_dia(radar)


def registrar_execucao(radar, inicio, status, erro=None):
    """Grava uma linha em execucoes_radar. Nunca derruba o radar se falhar."""
    try:
        fim = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            "radar": radar.nome,
            "iniciado_em": inicio.isoformat(),
            "finalizado_em": fim.isoformat(),
            "duracao_seg": round((fim - inicio).total_seconds(), 1),
            "editais_analisados": RESUMO_EXEC["analisadas"],
            "com_match": RESUMO_EXEC["encontradas"],
            "novos": RESUMO_EXEC["novas"],
            "buscas_com_erro": RESUMO_EXEC["buscas_com_erro"],
            "status": status,
            "erro": erro,
            "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        }
        if sb_gravar(radar, "POST", "execucoes_radar", json=payload, descricao=status):
            log.info(f"📝 Execução registrada em execucoes_radar ({status})")
    except Exception as e:
        log.warning(f"Não foi possível registrar execução: {e}")


def executar(radar):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    inicio = datetime.datetime.now(datetime.timezone.utc)
    status, erro = "ok", None
    try:
        main(radar)
    except SystemExit as e:
        if e.code not in (0, None):
            status, erro = "erro", f"sys.exit({e.code})"
        raise
    except Exception as e:
        status, erro = "erro", repr(e)[:500]
        raise
    finally:
        registrar_execucao(radar, inicio, status, erro)
