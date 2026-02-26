"""
RADAR FARMAUSA - Monitor de Licitações de Cannabis no PNCP (v2)
===============================================================
Busca licitações publicadas no Portal Nacional de Contratações Públicas
que contenham termos relacionados a cannabis/canabidiol no objeto da compra.

Correções v3:
- ATUALIZAR_DATAS: faz PATCH em registros existentes que estejam sem datas
- Permite rodar modo recuperação para preencher histórico sem re-enviar Telegram
"""

import os
import sys
import requests
import datetime
import time
import logging

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("radar")

# ================= CONFIGURAÇÕES =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not TELEGRAM_BOT_TOKEN:
    TELEGRAM_BOT_TOKEN = "8388155318:AAGrSb4FwLvAS51PZG4tmnapkM2V7p0lTYk"
if not CHAT_ID:
    CHAT_ID = "-5236299203"
if not SUPABASE_URL:
    SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
if not SUPABASE_KEY:
    SUPABASE_KEY = "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99"

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

KEYWORDS = [
    "canabidiol", "cannabis", "cbd", "cannabidiol",
    "thc", "cânhamo", "marijuana", "maconha medicinal"
]

BLOCKLIST_CONTEXTO = [
    "assistência técnica",
    "manutenção preventiva",
    "manutenção corretiva",
    "coffee break",
    "vale-transporte",
    "vale transporte",
    "bilhete digital",
    "serviços de limpeza",
    "material de escritório",
    "combustível",
]

def objeto_bloqueado(texto):
    if not texto:
        return False
    t = str(texto).lower()
    return any(b in t for b in BLOCKLIST_CONTEXTO)

PNCP_API_BASE = "https://pncp.gov.br/api/consulta/v1"

DIAS_RETROATIVOS = 1

APENAS_POPULAR_BANCO = False
DATA_ESPECIFICA = ""
ATUALIZAR_DATAS = True

MODALIDADES = [4, 5, 6, 7, 8, 9, 11, 13]
TAMANHO_PAGINA = 50
DASHBOARD_URL = "https://radar-farmausa.streamlit.app/"


# ================= UTILITÁRIOS =================

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

KEYWORDS_PALAVRA_INTEIRA = {"cbd", "thc"}

def keyword_match(texto):
    if not texto:
        return False
    import re
    texto_lower = str(texto).lower()
    for kw in KEYWORDS:
        if kw.lower() in KEYWORDS_PALAVRA_INTEIRA:
            pattern = r'(?<![a-zA-Z0-9])' + re.escape(kw.lower()) + r'(?![a-zA-Z])'
            if re.search(pattern, texto_lower):
                return True
        else:
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
        log.warning(f"Erro ao buscar datas individuais {cnpj}/{ano}/{seq}: {e}")
    return None, None


def buscar_itens_relevantes(cnpj, ano, seq):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens_texto = []
    itens_banco = []
    valor_total = 0.0

    try:
        r = requests.get(
            url,
            headers=PNCP_HEADERS,
            params={"pagina": 1, "tamanhoPagina": 500},
            timeout=15,
            allow_redirects=True
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
                    "vlr_unit": float(vlr_unit) if vlr_unit else None,
                })

                itens_banco.append({
                    "numero_item": str(num),
                    "descricao": str(desc)[:500],
                    "quantidade": float(qtd_raw) if qtd_raw else 0.0,
                    "valor_unitario": float(vlr_unit) if vlr_unit else None,
                    "valor_total": float(vlr_total) if vlr_total else None,
                })

        elif r.status_code == 204:
            pass
        else:
            log.warning(f"Itens API retornou {r.status_code} para {cnpj}/{ano}/{seq}")

    except Exception as e:
        log.warning(f"Erro ao buscar itens {cnpj}/{ano}/{seq}: {e}")

    return itens_texto, itens_banco, valor_total


def buscar_contratacoes_pagina(data_inicial, data_final, modalidade, pagina=1):
    url = f"{PNCP_API_BASE}/contratacoes/publicacao"
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade,
        "tamanhoPagina": TAMANHO_PAGINA,
        "pagina": pagina,
    }
    try:
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json().get("data", [])
        elif r.status_code == 204:
            return []
        else:
            log.warning(f"API PNCP {r.status_code} mod={modalidade} pag={pagina}")
            return []
    except requests.exceptions.Timeout:
        log.warning(f"Timeout mod={modalidade} pag={pagina}")
        return []
    except Exception as e:
        log.error(f"Erro API PNCP: {e}")
        return []


def buscar_todas_contratacoes(data_inicial, data_final, modalidade):
    todas = []
    pagina = 1
    while True:
        res = buscar_contratacoes_pagina(data_inicial, data_final, modalidade, pagina)
        if not res:
            break
        todas.extend(res)
        if len(res) < TAMANHO_PAGINA:
            break
        pagina += 1
        time.sleep(0.3)
        if pagina > 20:
            log.warning(f"Limite de páginas atingido mod={modalidade}")
            break
    return todas


# ================= SUPABASE =================

def check_and_save_supabase(dados_edital, dados_itens):
    url_id = dados_edital["url_id"]
    endpoint_editais = f"{SUPABASE_URL}/rest/v1/editais_pncp"
    endpoint_itens = f"{SUPABASE_URL}/rest/v1/itens_pncp"

    dados_para_salvar = {k: v for k, v in dados_edital.items() if not k.startswith("_")}

    try:
        check = requests.get(
            f"{endpoint_editais}?url_id=eq.{requests.utils.quote(url_id)}&select=url_id,data_inicio,data_fim,data_publicacao",
            headers=SUPABASE_HEADERS,
            timeout=10
        )
        if check.status_code == 200 and len(check.json()) > 0:
            registro = check.json()[0]

            if ATUALIZAR_DATAS:
                campos_atualizar = {}
                if not registro.get("data_inicio") and dados_para_salvar.get("data_inicio"):
                    campos_atualizar["data_inicio"] = dados_para_salvar["data_inicio"]
                if not registro.get("data_fim") and dados_para_salvar.get("data_fim"):
                    campos_atualizar["data_fim"] = dados_para_salvar["data_fim"]
                if not registro.get("data_publicacao") and dados_para_salvar.get("data_publicacao"):
                    campos_atualizar["data_publicacao"] = dados_para_salvar["data_publicacao"]

                if campos_atualizar:
                    patch = requests.patch(
                        f"{endpoint_editais}?url_id=eq.{requests.utils.quote(url_id)}",
                        headers={**SUPABASE_HEADERS, "Prefer": "return=minimal"},
                        json=campos_atualizar,
                        timeout=10
                    )
                    if patch.status_code in [200, 204]:
                        log.info(f"📅 Datas atualizadas: {url_id} → {list(campos_atualizar.keys())}")
                    else:
                        log.warning(f"Erro PATCH datas: {patch.status_code} {patch.text[:200]}")
                else:
                    log.info(f"⏭️  Já existe com datas completas: {url_id}")
            else:
                log.info(f"⏭️  Já existe no banco: {url_id}")

            return False
    except Exception as e:
        log.warning(f"Aviso ao verificar Supabase: {e}")

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
        log.info(f"✅ Supabase NOVO salvo: {url_id}")
    except Exception as e:
        log.error(f"ERRO request Supabase ({url_id}): {e}")
        return False

    if dados_itens:
        try:
            for item in dados_itens:
                item["edital_url_id"] = url_id
            res_itens = requests.post(
                endpoint_itens,
                headers={**SUPABASE_HEADERS, "Prefer": "return=minimal"},
                json=dados_itens,
                timeout=10
            )
            if res_itens.status_code not in [200, 201, 204]:
                log.warning(f"Erro ao inserir itens: {res_itens.status_code} - {res_itens.text[:200]}")
            else:
                log.info(f"   └─ {len(dados_itens)} item(ns) inserido(s)")
        except Exception as e:
            log.warning(f"Erro ao inserir itens: {e}")

    return True


# ================= TELEGRAM =================

def enviar_telegram(edital, itens_texto, modalidade_label):
    cnpj = edital.get("_cnpj", "")
    ano = edital.get("_ano", "")
    seq = edital.get("_seq", "")
    link_pncp = montar_link_pncp(cnpj, ano, seq) if cnpj else (
        f"https://pncp.gov.br/app/editais{edital.get('url_id', '')}"
    )

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
        f"🌿 <b>NOVA LICITAÇÃO — CANNABIS MEDICINAL</b>\n"
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
            log.info(f"🚀 Telegram enviado: {titulo[:60]}")
        else:
            log.error(f"Telegram erro {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.error(f"Telegram request falhou: {e}")


# ================= PROCESSAMENTO =================

def processar_contratacao(item):
    objeto = item.get("objetoCompra", "")

    if not keyword_match(objeto):
        return False

    if objeto_bloqueado(objeto):
        log.info(f"   🚫 Objeto bloqueado (contexto não farmacêutico): {objeto[:80]}")
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
        log.info(f"   🔍 Buscando datas individuais para {cnpj}/{ano}/{seq}...")
        di, df = buscar_datas_individuais(cnpj, ano, seq)
        if di:
            data_inicio = di
        if df:
            data_fim = df
        time.sleep(0.3)

    itens_texto, itens_banco, valor_itens = buscar_itens_relevantes(cnpj, ano, seq)
    time.sleep(0.3)

    if not itens_banco:
        log.info(f"   ⏭️  Objeto tinha keyword mas nenhum ITEM confirmou — ignorando")
        return False

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
        if APENAS_POPULAR_BANCO:
            log.info(f"   📦 Salvo no banco (modo recuperação — Telegram suprimido)")
        else:
            modalidade_label = classificar_modalidade(modalidade_nome)
            enviar_telegram(dados_edital, itens_texto, modalidade_label)
            time.sleep(0.5)
        return True

    return False



# ================= RESUMO DIÁRIO =================

def enviar_resumo_dia():
    hoje = datetime.date.today().isoformat()
    hoje_fmt = datetime.date.today().strftime("%d/%m/%Y")
    endpoint_editais = f"{SUPABASE_URL}/rest/v1/editais_pncp"
    endpoint_itens = f"{SUPABASE_URL}/rest/v1/itens_pncp"

    try:
        r = requests.get(
            endpoint_editais,
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
            },
            params={
                "data_publicacao": f"gte.{hoje}T00:00:00",
                "select": "url_id,orgao,uf,modalidade,valor_total_estimado,titulo",
                "order": "valor_total_estimado.desc.nullslast",
            },
            timeout=15,
        )
        editais = r.json() if r.status_code == 200 else []
    except Exception as e:
        log.warning(f"Erro ao buscar editais do dia: {e}")
        return

    if not editais:
        msg = (
            f"📊 <b>Resumo do dia — {hoje_fmt}</b>\n\n"
            f"Nenhuma licitação cannabis encontrada hoje."
        )
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.warning(f"Erro ao enviar resumo: {e}")
        return

    # Buscar itens de cada edital
    blocos = []
    valor_dia = 0.0

    for i, edital in enumerate(editais, 1):
        url_id = edital.get("url_id", "")
        orgao = edital.get("orgao", "N/A")
        uf = edital.get("uf", "")
        modalidade_nome = edital.get("modalidade", "")
        valor = edital.get("valor_total_estimado")

        if valor:
            try:
                valor_dia += float(valor)
            except Exception:
                pass

        modalidade_label = classificar_modalidade(modalidade_nome)
        cabecalho = f"{i}. {modalidade_label}"
        if uf:
            cabecalho += f" | {uf}"

        linhas = [cabecalho, f"🏢 {orgao}"]

        # Buscar itens do edital
        try:
            ri = requests.get(
                endpoint_itens,
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                },
                params={
                    "edital_url_id": f"eq.{url_id}",
                    "select": "descricao,quantidade,valor_unitario",
                },
                timeout=10,
            )
            itens = ri.json() if ri.status_code == 200 else []
        except Exception:
            itens = []

        for item in itens[:5]:
            desc = (item.get("descricao") or "")[:80]
            qtd = item.get("quantidade")
            vlr = item.get("valor_unitario")

            linha_item = f"   • {desc}"
            detalhes = []
            if qtd:
                try:
                    q = float(qtd)
                    detalhes.append(f"{int(q):,}".replace(",", ".") + " un.")
                except Exception:
                    pass
            if vlr:
                try:
                    v = float(vlr)
                    detalhes.append(f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + "/un.")
                except Exception:
                    pass
            if detalhes:
                linha_item += " — " + " | ".join(detalhes)
            linhas.append(linha_item)

        blocos.append("\n".join(linhas))

    separador = "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    corpo = separador.join(blocos)

    valor_fmt = ""
    if valor_dia > 0:
        valor_fmt = f"\n💰 Valor total do dia: R$ {valor_dia:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    msg = (
        f"📊 <b>Resumo do dia — {hoje_fmt}</b>\n"
        f"🌿 {len(editais)} licitação(ões) cannabis encontrada(s)\n"
        f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{corpo}"
        f"\n━━━━━━━━━━━━━━━━━━━━━━"
        f"{valor_fmt}\n"
        f'📊 <a href="{DASHBOARD_URL}">Ver no Radar FarmaUSA</a>'
    )

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        log.info(f"📊 Resumo diário enviado — {len(editais)} edital(is).")
    except Exception as e:
        log.warning(f"Erro ao enviar resumo diário: {e}")

# ================= MAIN =================

def main():
    agora = datetime.datetime.now()
    log.info("=" * 60)
    log.info(f"🔍 RADAR FARMAUSA v2 — Licitações Cannabis")
    log.info(f"⏰ Execução: {agora.strftime('%d/%m/%Y %H:%M:%S')}")
    log.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("❌ SUPABASE_URL ou SUPABASE_KEY não configurados!")
        sys.exit(1)
    if not TELEGRAM_BOT_TOKEN or not CHAT_ID:
        log.warning("⚠️ Telegram não configurado — alertas desativados")

    hoje = datetime.date.today()

    if DATA_ESPECIFICA:
        dia_alvo = datetime.datetime.strptime(DATA_ESPECIFICA, "%Y%m%d").date()
        dias = [dia_alvo]
        log.info(f"📅 Modo recuperação — Dia específico: {dia_alvo.strftime('%d/%m/%Y')}")
    else:
        # Primeira execução do dia (antes das 10h): busca ontem + hoje para garantir cobertura
        # Demais execuções: busca só hoje
        hora_atual = datetime.datetime.now().hour
        retroativos = 1 if hora_atual < 10 else 0
        data_ini = hoje - datetime.timedelta(days=retroativos)
        dias = []
        d = data_ini
        while d <= hoje:
            dias.append(d)
            d += datetime.timedelta(days=1)
        if retroativos > 0:
            log.info(f"📅 Período: {data_ini.strftime('%d/%m/%Y')} a {hoje.strftime('%d/%m/%Y')} (2 dias — cobertura da primeira execução)")
        else:
            log.info(f"📅 Período: hoje {hoje.strftime('%d/%m/%Y')} (execução intradiária)")

    log.info(f"🔑 Keywords: {', '.join(KEYWORDS)}")
    log.info(f"📊 Modalidades: {MODALIDADES}")
    log.info(f"📅 ATUALIZAR_DATAS: {ATUALIZAR_DATAS}")
    log.info(f"ℹ️  Existentes: {'atualiza datas faltantes' if ATUALIZAR_DATAS else 'ignora'}")

    total_analisadas = 0
    total_encontradas = 0
    total_novas = 0

    for dia in dias:
        dia_str = dia.strftime("%Y%m%d")
        log.info(f"\n📆 Processando dia: {dia.strftime('%d/%m/%Y')}")

        for modalidade in MODALIDADES:
            contratacoes = buscar_todas_contratacoes(dia_str, dia_str, modalidade)
            if contratacoes:
                log.info(f"   Mod {modalidade}: {len(contratacoes)} contratações")
            total_analisadas += len(contratacoes)

            for item in contratacoes:
                if keyword_match(item.get("objetoCompra", "")):
                    total_encontradas += 1
                    log.info(f"   🎯 Match: {item.get('objetoCompra', '')[:80]}...")
                    if processar_contratacao(item):
                        total_novas += 1

            time.sleep(0.3)

    log.info("=" * 60)
    log.info("📊 RESUMO:")
    log.info(f"   Analisadas:        {total_analisadas}")
    log.info(f"   Com cannabis:      {total_encontradas}")
    log.info(f"   Novas (Telegram):  {total_novas}")
    log.info(f"   Já conhecidas:     {total_encontradas - total_novas}")
    log.info("=" * 60)

    # Resumo diário — envia apenas na execução das 18h em diante
    if TELEGRAM_BOT_TOKEN and CHAT_ID and not APENAS_POPULAR_BANCO:
        if datetime.datetime.now().hour >= 18:
            enviar_resumo_dia()


if __name__ == "__main__":
    main()
