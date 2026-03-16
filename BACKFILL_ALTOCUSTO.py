"""
BACKFILL ALTO CUSTO - Varredura Retroativa 2026
=================================================
Percorre dia a dia de 01/01/2026 até hoje, buscando na API Search do PNCP
licitações com keywords de alto custo.

- Salva no Supabase (editais_pncp_altocusto / itens_pncp_altocusto)
- Envia alerta no Telegram para cada licitação NOVA encontrada
- Pula registros que já existem no banco (não duplica)
- Checkpoint: salva o último dia processado para poder retomar se interromper
"""

import os
import sys
import requests
import datetime
import time
import logging
import json

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("backfill_altocusto")

# ================= CONFIGURAÇÕES =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_ALTOCUSTO", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not TELEGRAM_BOT_TOKEN:
    TELEGRAM_BOT_TOKEN = "8388155318:AAGrSb4FwLvAS51PZG4tmnapkM2V7p0lTYk"
if not CHAT_ID:
    CHAT_ID = "-5180338942"
if not SUPABASE_URL:
    SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
if not SUPABASE_KEY:
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNsY2FveXJxaGt4aXJmZWtjeG90Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE4NzUzNTUsImV4cCI6MjA4NzQ1MTM1NX0.vwTN5ACAylYO-FMQ5iEubJ2I-vZ9YMUHG-7pXk2YIOg"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

PNCP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# ================= TABELAS =================
TABELA_EDITAIS = "editais_pncp_altocusto"
TABELA_ITENS = "itens_pncp_altocusto"

# ================= PERÍODO =================
DATA_INICIO = "2026-01-01"
# DATA_FIM será hoje automaticamente

# Enviar no Telegram? True = envia alertas, False = só salva no banco
ENVIAR_TELEGRAM = True

# ================= KEYWORDS =================
KEYWORDS = [
    "77vf", "Proteina C Composicao Humana", "Proteinas C Composicoes Humanas",
    "Abilify", "Acalabrutinib", "Acalabrutinibe", "Acamprosate", "Acamprosato",
    "Acido Quenodesoxicolico", "Adeek", "Adek", "Adenuric", "Alafenamida",
    "Alectinibe", "Alfapeginterferona", "Alfassebelipase", "Ammonaps", "Amvuttra",
    "Anacinra", "Anagrelida", "Anakinra", "Aquadeks", "Aquae", "Asenapine",
    "Atomoxetina", "Atomoxetine", "Atriance", "Axitinibe", "Balsalazide",
    "Benzatropina", "Benztropine", "Betaciclodextrina", "Betanecol", "Biktarvy",
    "Braftovi", "Brentuximab", "Brevactid", "Brivaracetam", "Briviact", "Campral",
    "Camzyos", "Cenobamato",
    "Cevimelina", "Cevimeline", "Champix", "Charlotte", "Chenodal", "Chenodiol",
    "Cibinqo", "Cidofovir", "Cisteamina", "Cortef", "Cortrosyn", "Cosintropina",
    "Cosmegen", "Cosyntropin", "Cystagon", "Cytomel", "Dactinomicina", "Dantrium",
    "Dantroleno", "Daratumumabe", "Dayvigo", "Dekas", "Diacomit", "Diamox",
    "Diazoxido", "Dietilpropiona", "Dificlir", "Dissulfiram", "Edastar",
    "Eflornithine", "Elixinol", "Elmiron", "Enjaymo", "Epipen",
    "Escopolamina Adesivo", "Estiripentol", "Ethosuximide", "Etossuximida", "Evr",
    "Farmausa", "Febuxostat", "Febuxostato", "Felbamate", "Felbamato",
    "Fenilbutirato Sodio", "Fidaxomicin", "Firdapse", "Flecainide", "Flucitosina",
    "Fomepizol", "Foscarnet", "Foscavir", "Gamainterferona", "Givinostat",
    "Glanatec", "Gonadorelin", "Gonadotropina", "Guanfacina", "Guanfacine",
    "Gutron", "Hemina", "Hepazec", "Hialuronato", "Hidroxocobalamin", "Hioscina",
    "Hisone", "Hodpro", "Ibrutinib", "Idebenone", "Imbruvica", "Imukin",
    "Inbrija", "Increlex", "Inotersena", "Inovelon", "Intralipid", "Intuniv",
    "Isodiolex", "Jaypirca", "Kaliumbromid", "Kayexalate", "Kenalog", "Kimmtrak",
    "Kineret", "Lecanemab", "Lemborexant", "Leqembi", "Leukine", "Liotironina",
    "Lokelma", "Lomoother", "Lomustina", "Ludiomil", "Macitentana", "Maprotilina",
    "Mechlorethamine", "Medrosan", "Megamilbedoce", "Melphalan Mesylate",
    "Mexiletina", "Mexiletine", "Midodrina", "Midodrine", "Mirvetuximabe",
    "Misintu", "Mitomycin", "Mounjaro", "Nabix", "Nadolol", "Natulan",
    "Nelarabina", "Nintedanibe", "Nitisinona", "Nivolumabe", "Ocrelizumabe",
    "Omalizumabe", "Ontozry", "Opdualag", "Ospolot", "Pembrolizumabe", "Penepin",
    "Penfluridol", "Penridol", "Pentosana", "Perfenazina", "Perphenazine",
    "Pimozida", "Pimozide", "Pirtobrutinibe", "Pluvicto", "Pomalid",
    "Procarbazina", "Proglicem", "Proglycem", "Purodiol", "Pyrukynd",
    "Quenodesoxicolico", "Quenodiol", "Quinidina", "Quinidine", "Quvivq",
    "Rapaflo", "Relyvrio", "Revocon", "Ripasudil", "Rsho", "Rufinamida",
    "Ruxolitinibe", "Saphris", "Scopoderm", "Sidovis", "Smoflipid",
    "Soravtansina", "Strattera", "Sulindaco", "Sulthiame", "Synacthen", "Taloxa",
    "Tanganil", "Tegsedi", "Tepadina", "Tetrabenazine", "Tetracosactina",
    "Thiola", "Thiotepa", "Tiopronina", "Tirzepatide", "Trabec",
    "Tranilcipromina", "Trastuzumabe Deruxtecana", "Trientina", "Trientine",
    "Trikafta", "Tryprine", "Tudca", "Vabysmo", "Vaniqa", "Vareniclina",
    "Varenicline", "Vepesid", "Vimizim", "Vivjoa", "Xagrid", "Xenazine",
    "Zanubrutinib", "Zonisamida", "Zonisamide", "Ztalmy",
]

KEYWORDS_BUSCA = [
    "pembrolizumabe", "nivolumabe", "ruxolitinibe", "ocrelizumabe",
    "daratumumabe", "trastuzumabe deruxtecana", "brentuximab",
    "ibrutinib", "zanubrutinib", "acalabrutinibe",
    "tirzepatide", "mounjaro", "omalizumabe",
    "nintedanibe", "axitinibe", "alectinibe",
    "trikafta", "vabysmo", "lecanemab",
    "dactinomicina", "lomustina", "nelarabina",
    "anakinra", "atomoxetina", "brivaracetam",
    "etossuximida", "rufinamida", "cenobamato",
    "dissulfiram", "vareniclina", "liotironina",
    "midodrina", "dantroleno", "cisteamina",
    "fomepizol", "cidofovir", "foscarnet",
    "pluvicto", "mirvetuximabe", "pirtobrutinibe",
]

BLOCKLIST_CONTEXTO = [
    "assistência técnica", "manutenção preventiva", "manutenção corretiva",
    "coffee break", "vale-transporte", "vale transporte", "bilhete digital",
    "serviços de limpeza", "material de escritório", "combustível",
]

DASHBOARD_URL = "https://radar-farmausa.streamlit.app/"
CHECKPOINT_FILE = "/tmp/backfill_altocusto_checkpoint.txt"


# ================= UTILITÁRIOS =================

def objeto_bloqueado(texto):
    if not texto:
        return False
    t = str(texto).lower()
    return any(b in t for b in BLOCKLIST_CONTEXTO)


def formatar_data_br(data_iso):
    if not data_iso:
        return None
    try:
        dt = datetime.datetime.fromisoformat(data_iso.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(data_iso)


def calcular_dias_restantes(data_fim_iso):
    if not data_fim_iso:
        return None, None
    try:
        dt = datetime.datetime.fromisoformat(data_fim_iso.replace("Z", "+00:00"))
        if dt.tzinfo:
            dt = dt.replace(tzinfo=None)
        diff = dt - datetime.datetime.now()
        total_horas = diff.total_seconds() / 3600
        dias = int(total_horas // 24)
        horas = int(total_horas % 24)
        return dias, horas
    except Exception:
        return None, None


def formatar_prazo(data_fim_iso):
    dias, horas = calcular_dias_restantes(data_fim_iso)
    if dias is None:
        return None
    if dias < 0:
        return f"Encerrado há {abs(dias)}d"
    elif dias == 0:
        return f"⚠️ HOJE — {horas}h restantes"
    elif dias == 1:
        return f"⚠️ AMANHÃ — {horas}h restantes"
    elif dias <= 3:
        return f"⚠️ {dias}d {horas}h restantes"
    else:
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
    if val is None:
        return None
    try:
        f = float(val)
        if f <= 0:
            return None
        return f"R$ {f:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return None


def keyword_match(texto):
    if not texto:
        return False
    texto_lower = str(texto).lower()
    for kw in KEYWORDS:
        if kw.lower() in texto_lower:
            return True
    return False


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


def salvar_checkpoint(data_str):
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(data_str)
    except Exception:
        pass


def carregar_checkpoint():
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    except Exception:
        return None


# ================= API PNCP =================

def buscar_datas_individuais(cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}"
    try:
        r = requests.get(url, headers=PNCP_HEADERS, timeout=15, allow_redirects=True)
        if r.status_code == 200:
            dados = r.json()
            if isinstance(dados, dict):
                return (
                    dados.get("dataAberturaProposta"),
                    dados.get("dataEncerramentoProposta")
                )
    except Exception as e:
        log.warning(f"Erro datas individuais {cnpj}/{ano}/{seq}: {e}")
    return None, None


def buscar_itens_relevantes(cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens_texto = []
    itens_banco = []
    valor_total = 0.0

    try:
        r = requests.get(
            url, headers=PNCP_HEADERS,
            params={"pagina": 1, "tamanhoPagina": 500},
            timeout=15, allow_redirects=True
        )
        if r.status_code == 200:
            dados = r.json()
            lista = dados if isinstance(dados, list) else dados.get("data", dados.get("items", []))

            for item in lista:
                desc = item.get("descricao", item.get("materialOuServicoNome", ""))
                if not keyword_match(desc):
                    continue

                num = item.get("numeroItem", "-")
                qtd_raw = item.get("quantidade", 0)
                vlr_unit = item.get("valorUnitarioEstimado")
                vlr_total = item.get("valorTotalEstimado")

                if vlr_total:
                    try:
                        valor_total += float(vlr_total)
                    except Exception:
                        pass
                elif vlr_unit and qtd_raw:
                    try:
                        valor_total += float(vlr_unit) * float(qtd_raw)
                    except Exception:
                        pass

                qtd_fmt = formatar_qtd(qtd_raw)
                unit_fmt = formatar_valor(vlr_unit)
                total_fmt = formatar_valor(vlr_total)

                linha_parts = [f"📦 {qtd_fmt} un." if qtd_fmt else ""]
                if unit_fmt:
                    linha_parts.append(f"💲 {unit_fmt}/un.")
                if total_fmt:
                    linha_parts.append(f"💰 Total: {total_fmt}")

                linha = " | ".join(p for p in linha_parts if p)

                itens_texto.append({
                    "numero": num,
                    "descricao": str(desc)[:150],
                    "linha": linha,
                })

                itens_banco.append({
                    "numero_item": str(num),
                    "descricao": str(desc)[:500],
                    "quantidade": float(qtd_raw) if qtd_raw else 0.0,
                    "valor_unitario": float(vlr_unit) if vlr_unit else None,
                    "valor_total": float(vlr_total) if vlr_total else None,
                })

    except Exception as e:
        log.warning(f"Erro itens {cnpj}/{ano}/{seq}: {e}")

    return itens_texto, itens_banco, valor_total


# ─── API SEARCH ─────────

def buscar_search_api(keyword, data_fmt, pagina=1, tam_pagina=20):
    url = "https://pncp.gov.br/api/search/"
    params = {
        "q": keyword,
        "tipos_documento": "edital",
        "ordenacao": "-data",
        "pagina": pagina,
        "tam_pagina": tam_pagina,
    }
    try:
        r = requests.get(url, params=params, headers=PNCP_HEADERS, timeout=30)
        if r.status_code == 200:
            dados = r.json()
            items = dados.get("items", [])
            filtrados = [
                i for i in items
                if (i.get("data_publicacao_pncp") or "").startswith(data_fmt)
            ]
            return filtrados
        return []
    except requests.exceptions.Timeout:
        log.warning(f"Timeout search '{keyword}' pag={pagina}")
        return []
    except Exception as e:
        log.error(f"Erro search: {e}")
        return []


def normalizar_item_search(item):
    cnpj = item.get("orgao_cnpj", "")
    ano = item.get("ano", "")
    seq = item.get("numero_sequencial", "")
    return {
        "objetoCompra": item.get("description", item.get("title", "")),
        "orgaoEntidade": {
            "cnpj": cnpj,
            "razaoSocial": item.get("orgao_nome", ""),
        },
        "unidadeOrgao": {
            "ufSigla": item.get("uf", ""),
        },
        "anoCompra": ano,
        "sequencialCompra": seq,
        "modalidadeNome": item.get("modalidade_licitacao_nome", ""),
        "dataPublicacaoPncp": item.get("data_publicacao_pncp"),
        "dataAberturaProposta": item.get("data_inicio_vigencia"),
        "dataEncerramentoProposta": item.get("data_fim_vigencia"),
        "valorTotalEstimado": item.get("valor_total_estimado"),
        "numeroCompra": item.get("numero_sequencial", ""),
        "numeroControlePNCP": item.get("item_url", ""),
        "linkSistemaOrigem": "",
        "_source": "search",
    }


def buscar_por_search(data_str):
    data_fmt = datetime.datetime.strptime(data_str, "%Y%m%d").strftime("%Y-%m-%d")
    todos = {}

    for kw in KEYWORDS_BUSCA:
        pagina = 1
        while True:
            items = buscar_search_api(kw, data_fmt, pagina=pagina)
            for item in items:
                cnpj = item.get("orgao_cnpj", "")
                ano = item.get("ano", "")
                seq = item.get("numero_sequencial", "")
                if cnpj and ano and seq:
                    url_id = f"/compras/{cnpj}/{ano}/{seq}"
                    if url_id not in todos:
                        todos[url_id] = normalizar_item_search(item)
            if len(items) < 20:
                break
            pagina += 1
            time.sleep(0.2)
            if pagina > 10:
                break

    return list(todos.values())


# ================= SUPABASE =================

def check_and_save_supabase(dados_edital, dados_itens):
    url_id = dados_edital["url_id"]
    endpoint_editais = f"{SUPABASE_URL}/rest/v1/{TABELA_EDITAIS}"
    endpoint_itens = f"{SUPABASE_URL}/rest/v1/{TABELA_ITENS}"

    dados_para_salvar = {k: v for k, v in dados_edital.items() if not k.startswith("_")}

    try:
        check = requests.get(
            f"{endpoint_editais}?url_id=eq.{requests.utils.quote(url_id)}&select=url_id",
            headers=SUPABASE_HEADERS,
            timeout=10
        )
        if check.status_code == 200 and len(check.json()) > 0:
            log.info(f"⏭️  Já existe: {url_id}")
            return False
    except Exception as e:
        log.warning(f"Aviso verificação Supabase: {e}")

    headers_upsert = {**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}
    try:
        res = requests.post(
            f"{endpoint_editais}?on_conflict=url_id",
            headers=headers_upsert,
            json=dados_para_salvar,
            timeout=10
        )
        if res.status_code not in [200, 201, 204]:
            log.error(f"ERRO upsert ({url_id}): {res.status_code} - {res.text[:300]}")
            return False
        log.info(f"✅ NOVO salvo: {url_id}")
    except Exception as e:
        log.error(f"ERRO Supabase ({url_id}): {e}")
        return False

    if dados_itens:
        try:
            for item in dados_itens:
                item["edital_url_id"] = url_id
            requests.post(
                endpoint_itens,
                headers={**SUPABASE_HEADERS, "Prefer": "return=minimal"},
                json=dados_itens,
                timeout=10
            )
        except Exception as e:
            log.warning(f"Erro itens: {e}")

    return True


# ================= TELEGRAM =================

def enviar_telegram(edital, itens_texto, modalidade_label):
    if not ENVIAR_TELEGRAM:
        return

    cnpj = edital.get("_cnpj", "")
    ano = edital.get("_ano", "")
    seq = edital.get("_seq", "")
    link_pncp = montar_link_pncp(cnpj, ano, seq)

    uf = edital.get("uf", "")
    orgao = edital.get("orgao", "N/A")
    titulo = edital.get("titulo", "N/A")

    linha_local = f"{modalidade_label}  |  {uf}" if uf else modalidade_label

    bloco_produtos = ""
    if itens_texto:
        linhas = []
        for it in itens_texto[:5]:
            linhas.append(f"• Item {it['numero']} — {it['descricao'][:100]}")
            if it["linha"]:
                linhas.append(f"  ↳ {it['linha']}")
        bloco_produtos = "\n\n💊 <b>PRODUTO(S):</b>\n" + "\n".join(linhas)

    dt_inicio_fmt = formatar_data_br(edital.get("data_inicio"))
    dt_fim_fmt = formatar_data_br(edital.get("data_fim"))
    prazo_txt = formatar_prazo(edital.get("data_fim"))

    linhas_prazo = []
    if dt_inicio_fmt:
        linhas_prazo.append(f"🟢 Abertura:      {dt_inicio_fmt}")
    if dt_fim_fmt:
        sufixo = f"  ← {prazo_txt}" if prazo_txt else ""
        linhas_prazo.append(f"🔴 Encerramento:  {dt_fim_fmt}{sufixo}")

    bloco_prazos = ""
    if linhas_prazo:
        bloco_prazos = "\n\n⏱ <b>PRAZOS:</b>\n" + "\n".join(linhas_prazo)

    msg = (
        f"💊 <b>LICITAÇÃO ALTO CUSTO (retroativa)</b>\n"
        f"{linha_local}\n\n"
        f"🏢 {orgao}\n"
        f"📋 {titulo}"
        f"{bloco_produtos}"
        f"{bloco_prazos}\n\n"
        f"🔗 <a href=\"{link_pncp}\">Abrir Edital no PNCP</a>\n"
        f"📊 <a href=\"{DASHBOARD_URL}\">Ver no Radar FarmaUSA</a>"
    )

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            log.info(f"🚀 Telegram: {titulo[:60]}")
        elif resp.status_code == 429:
            # Rate limit do Telegram — esperar e tentar de novo
            retry_after = 5
            try:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
            except Exception:
                pass
            log.warning(f"⏳ Telegram rate limit — aguardando {retry_after}s")
            time.sleep(retry_after + 1)
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=10,
            )
        else:
            log.error(f"Telegram erro {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.error(f"Telegram falhou: {e}")

    # Pausa entre mensagens para não tomar rate limit
    time.sleep(1.5)


# ================= PROCESSAMENTO =================

def processar_contratacao(item):
    objeto = item.get("objetoCompra", "")
    from_search = item.get("_source") == "search"

    if not from_search and not keyword_match(objeto):
        return False

    if objeto_bloqueado(objeto):
        log.info(f"   🚫 Bloqueado: {objeto[:80]}")
        return False

    cnpj = item.get("orgaoEntidade", {}).get("cnpj", "")
    ano = item.get("anoCompra")
    seq = item.get("sequencialCompra")

    if not cnpj or not ano or not seq:
        return False

    url_id = f"/compras/{cnpj}/{ano}/{seq}"

    data_inicio = item.get("dataAberturaProposta")
    data_fim = item.get("dataEncerramentoProposta")

    if not data_inicio or not data_fim:
        di, df = buscar_datas_individuais(cnpj, ano, seq)
        if di:
            data_inicio = di
        if df:
            data_fim = df
        time.sleep(0.3)

    itens_texto, itens_banco, valor_itens = buscar_itens_relevantes(cnpj, ano, seq)
    time.sleep(0.3)

    if not itens_banco and from_search:
        log.info(f"   ⚠️  Itens vazio mas origem search — salvando")

    valor_listagem = item.get("valorTotalEstimado")
    try:
        valor_total = valor_itens if valor_itens > 0 else (float(valor_listagem) if valor_listagem else 0.0)
    except Exception:
        valor_total = 0.0

    modalidade_nome = item.get("modalidadeNome", "")
    numero_edital = item.get("numeroCompra", item.get("sequencialCompra", ""))
    titulo = f"Edital nº {numero_edital}" if numero_edital else objeto[:200]

    dados_edital = {
        "url_id": url_id,
        "titulo": titulo,
        "objeto": objeto[:500],
        "orgao": item.get("orgaoEntidade", {}).get("razaoSocial", ""),
        "uf": (
            item.get("unidadeOrgao", {}).get("ufSigla", "") or
            item.get("orgaoEntidade", {}).get("ufSigla", "")
        ),
        "modalidade": modalidade_nome,
        "data_publicacao": item.get("dataPublicacaoPncp"),
        "valor_total_estimado": valor_total if valor_total > 0 else None,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "numero_controle_pncp": item.get("numeroControlePNCP", ""),
        "link_sistema_origem": item.get("linkSistemaOrigem", ""),
        "_cnpj": cnpj,
        "_ano": str(ano),
        "_seq": str(seq),
    }

    if check_and_save_supabase(dados_edital, itens_banco):
        modalidade_label = classificar_modalidade(modalidade_nome)
        enviar_telegram(dados_edital, itens_texto, modalidade_label)
        return True

    return False


# ================= MAIN =================

def main():
    agora = datetime.datetime.now()
    log.info("=" * 60)
    log.info("💊 BACKFILL ALTO CUSTO — Varredura Retroativa 2026")
    log.info(f"⏰ Início: {agora.strftime('%d/%m/%Y %H:%M:%S')}")
    log.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("❌ SUPABASE_URL ou SUPABASE_KEY não configurados!")
        sys.exit(1)

    hoje = datetime.date.today()
    data_ini = datetime.datetime.strptime(DATA_INICIO, "%Y-%m-%d").date()

    # Verificar checkpoint para retomar de onde parou
    checkpoint = carregar_checkpoint()
    if checkpoint:
        try:
            data_retomada = datetime.datetime.strptime(checkpoint, "%Y-%m-%d").date()
            data_retomada += datetime.timedelta(days=1)  # começa do dia seguinte
            if data_retomada > data_ini:
                log.info(f"📌 Retomando do checkpoint: {data_retomada.strftime('%d/%m/%Y')}")
                data_ini = data_retomada
        except Exception:
            pass

    # Gerar lista de dias
    dias = []
    d = data_ini
    while d <= hoje:
        dias.append(d)
        d += datetime.timedelta(days=1)

    log.info(f"📅 Período: {data_ini.strftime('%d/%m/%Y')} a {hoje.strftime('%d/%m/%Y')}")
    log.info(f"📆 Total de dias: {len(dias)}")
    log.info(f"🔑 Keywords busca: {len(KEYWORDS_BUSCA)} termos")
    log.info(f"📱 Telegram: {'ATIVADO' if ENVIAR_TELEGRAM else 'DESATIVADO'}")
    log.info("-" * 60)

    total_geral = 0
    total_novas = 0

    for i, dia in enumerate(dias, 1):
        dia_str = dia.strftime("%Y%m%d")
        dia_fmt = dia.strftime("%d/%m/%Y")
        log.info(f"\n📆 [{i}/{len(dias)}] {dia_fmt}")

        contratacoes = buscar_por_search(dia_str)
        log.info(f"   📋 {len(contratacoes)} editais encontrados")
        total_geral += len(contratacoes)

        novas_dia = 0
        for item in contratacoes:
            if processar_contratacao(item):
                novas_dia += 1
                total_novas += 1

        if novas_dia > 0:
            log.info(f"   ✅ {novas_dia} novas salvas neste dia")

        salvar_checkpoint(dia.strftime("%Y-%m-%d"))
        time.sleep(0.5)

    log.info("\n" + "=" * 60)
    log.info("📊 RESUMO FINAL BACKFILL:")
    log.info(f"   Dias processados:    {len(dias)}")
    log.info(f"   Editais encontrados: {total_geral}")
    log.info(f"   Novos salvos:        {total_novas}")
    log.info("=" * 60)

    # Mensagem final no Telegram
    if ENVIAR_TELEGRAM and TELEGRAM_BOT_TOKEN and CHAT_ID:
        msg = (
            f"📊 <b>Backfill Alto Custo — Concluído</b>\n\n"
            f"📅 Período: {datetime.datetime.strptime(DATA_INICIO, '%Y-%m-%d').strftime('%d/%m/%Y')} a {hoje.strftime('%d/%m/%Y')}\n"
            f"📆 Dias processados: {len(dias)}\n"
            f"📋 Editais encontrados: {total_geral}\n"
            f"🆕 Novos salvos: <b>{total_novas}</b>"
        )
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception:
            pass


if __name__ == "__main__":
    main()
