"""
RADAR FARMAUSA - Monitor de Licitações de Cannabis no PNCP (v2)
===============================================================
Busca licitações publicadas no Portal Nacional de Contratações Públicas
que contenham termos relacionados a cannabis/canabidiol no objeto da compra.

Estratégia:
- A API do PNCP NÃO tem busca por texto. Busca por data + modalidade.
- Filtra pelo campo "objetoCompra" usando as keywords definidas.
- Quando a API de listagem não retorna datas, busca no endpoint individual.
- Salva no Supabase (upsert) e envia alerta no Telegram apenas para novos registros.

Correções v2:
- Endpoint de itens: /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens
  (v1 usava /api/pncp/v1/... que retorna 301)
- Busca de datas individuais quando a listagem retorna null
- Link PNCP correto: /app/editais/{cnpj}/{ano}/{seq}  (sem /compras/ no meio)
- Novo formato de mensagem Telegram: mais limpo, dias restantes, V. Unit em destaque
- Qtd formatada sem .0 desnecessário
- Valor sigiloso omitido em vez de exibir "Sigiloso/Não inf."
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

# Fallback local (remover em produção)
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

# Keywords para filtrar no campo objetoCompra
KEYWORDS = [
    "canabidiol", "cannabis", "cbd", "cannabidiol",
    "thc", "cânhamo", "marijuana", "maconha medicinal"
]

PNCP_API_BASE = "https://pncp.gov.br/api/consulta/v1"

DIAS_RETROATIVOS = 15

# Modalidades: 4=Concorrência Eletrônica, 5=Concorrência Presencial,
# 6=Pregão Eletrônico, 7=Pregão Presencial, 8=Dispensa,
# 9=Inexigibilidade, 11=Credenciamento, 13=IRP
MODALIDADES = [4, 5, 6, 7, 8, 9, 11, 13]

TAMANHO_PAGINA = 50

DASHBOARD_URL = "https://radar-farmausa.streamlit.app/"


# ================= UTILITÁRIOS =================

def formatar_data_br(data_iso):
    """Converte ISO datetime para dd/mm/yyyy HH:MM."""
    if not data_iso:
        return None
    try:
        dt = datetime.datetime.fromisoformat(data_iso.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(data_iso)


def calcular_dias_restantes(data_fim_iso):
    """Retorna (dias, horas) restantes até o encerramento. Negativo = encerrado."""
    if not data_fim_iso:
        return None, None
    try:
        dt = datetime.datetime.fromisoformat(data_fim_iso.replace("Z", "+00:00"))
        # Remove timezone para comparar com datetime.now()
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
    """Retorna texto humanizado do prazo restante."""
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
    """Formata quantidade sem casas decimais desnecessárias."""
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
    """Formata valor monetário em reais. Retorna None se sigiloso/ausente."""
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
    """Verifica se alguma keyword aparece no texto (case insensitive)."""
    if not texto:
        return False
    texto_lower = str(texto).lower()
    return any(kw.lower() in texto_lower for kw in KEYWORDS)


def montar_link_pncp(cnpj, ano, seq):
    """Monta o link correto do PNCP para o edital."""
    return f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"


def classificar_modalidade(modalidade_nome):
    """Retorna emoji + label da modalidade para o Telegram."""
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
    """
    Busca datas de abertura/encerramento no endpoint individual do edital.
    Usado como fallback quando a listagem retorna datas nulas.

    Endpoint correto: /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}
    """
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
    """
    Busca itens de uma compra e filtra os relevantes por keyword.

    Endpoint correto: /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens
    (v1 usava /api/pncp/v1/... que retornava 301)

    Retorna: (lista_itens_telegram, lista_itens_banco, valor_total_float)
    """
    url = f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
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

                # Acumula valor total (para salvar no edital)
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

                # Monta linha para o Telegram
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
            pass  # sem itens
        else:
            log.warning(f"Itens API retornou {r.status_code} para {cnpj}/{ano}/{seq}")

    except Exception as e:
        log.warning(f"Erro ao buscar itens {cnpj}/{ano}/{seq}: {e}")

    return itens_texto, itens_banco, valor_total


def buscar_contratacoes_pagina(data_inicial, data_final, modalidade, pagina=1):
    """Consulta /v1/contratacoes/publicacao para uma página."""
    url = f"{PNCP_API_BASE}/contratacoes/publicacao"
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade,
        "tamanhoPagina": TAMANHO_PAGINA,
        "pagina": pagina,
    }
    try:
        r = requests.get(url, params=params, timeout=20)
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
    """Busca todas as páginas de uma modalidade."""
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
    """
    Upsert do edital no Supabase.
    Retorna True se NOVO (dispara Telegram), False se já existia.
    """
    url_id = dados_edital["url_id"]
    endpoint_editais = f"{SUPABASE_URL}/rest/v1/editais_pncp"
    endpoint_itens = f"{SUPABASE_URL}/rest/v1/itens_pncp"

    # Verifica se já existe
    is_new = True
    try:
        check = requests.get(
            f"{endpoint_editais}?url_id=eq.{requests.utils.quote(url_id)}&select=url_id",
            headers=SUPABASE_HEADERS,
            timeout=10
        )
        if check.status_code == 200 and len(check.json()) > 0:
            is_new = False
    except Exception as e:
        log.warning(f"Aviso ao verificar Supabase: {e}")

    # Upsert edital
    headers_upsert = SUPABASE_HEADERS.copy()
    headers_upsert["Prefer"] = "resolution=merge-duplicates"
    try:
        res = requests.post(
            f"{endpoint_editais}?on_conflict=url_id",
            headers=headers_upsert,
            json=dados_edital,
            timeout=10
        )
        if res.status_code not in [200, 201, 204]:
            log.error(f"ERRO upsert ({url_id}): {res.status_code} - {res.text}")
            return False
        log.info(f"✅ Supabase {'NOVO' if is_new else 'ATUALIZADO'}: {url_id}")
    except Exception as e:
        log.error(f"ERRO request Supabase ({url_id}): {e}")
        return False

    # Insere itens apenas se for registro novo
    if is_new and dados_itens:
        try:
            for item in dados_itens:
                item["edital_url_id"] = url_id
            requests.post(endpoint_itens, headers=SUPABASE_HEADERS, json=dados_itens, timeout=10)
            log.info(f"   └─ {len(dados_itens)} item(ns) inserido(s)")
        except Exception as e:
            log.warning(f"Erro ao inserir itens: {e}")

    return is_new


# ================= TELEGRAM =================

def enviar_telegram(edital, itens_texto, modalidade_label):
    """
    Envia alerta formatado no Telegram.

    Formato:
    🌿 NOVA LICITAÇÃO — CANNABIS MEDICINAL
    📈 Pregão  |  SP • São Paulo
    🏢 Secretaria de Estado da Saúde
    📋 Edital nº 002/2026

    💊 PRODUTO(S):
    Item 1 — Canabidiol 200mg/ml frasco 30ml
    📦 500 un. | 💲 R$ 659,24/un.

    ⏱ PRAZOS:
    🟢 Abertura:      24/02/2026 18:00
    🔴 Encerramento:  05/03/2026 09:00  ← 8 dias restantes

    🔗 Abrir Edital no PNCP
    📊 Ver no Radar FarmaUSA
    """
    cnpj = edital.get("_cnpj", "")
    ano = edital.get("_ano", "")
    seq = edital.get("_seq", "")
    link_pncp = montar_link_pncp(cnpj, ano, seq) if cnpj else (
        f"https://pncp.gov.br/app/editais{edital.get('url_id', '')}"
    )

    uf = edital.get("uf", "")
    orgao = edital.get("orgao", "N/A")
    titulo = edital.get("titulo", "N/A")

    # Cabeçalho: modalidade | UF
    linha_local = f"{modalidade_label}  |  {uf}" if uf else modalidade_label

    # Bloco de produtos
    bloco_produtos = ""
    if itens_texto:
        linhas = []
        for it in itens_texto[:5]:  # máx 5 itens
            linhas.append(f"• Item {it['numero']} — {it['descricao'][:100]}")
            if it["linha"]:
                linhas.append(f"  ↳ {it['linha']}")
        bloco_produtos = "\n\n💊 <b>PRODUTO(S):</b>\n" + "\n".join(linhas)

    # Bloco de prazos
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
        f"📊 <a href=\"{DASHBOARD_URL}\">Ver no Radar FarmaUSA</a>\n"
        f"<i>Alerta automático - FarmaUSA Life</i>"
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
    """
    Processa uma contratação retornada pela API.
    Verifica keywords, busca itens, busca datas se ausentes, salva e notifica.
    """
    objeto = item.get("objetoCompra", "")
    if not keyword_match(objeto):
        return False

    cnpj = item.get("orgaoEntidade", {}).get("cnpj", "")
    ano = item.get("anoCompra")
    seq = item.get("sequencialCompra")

    if not cnpj or not ano or not seq:
        return False

    url_id = f"/compras/{cnpj}/{ano}/{seq}"

    # Datas da listagem
    data_inicio = item.get("dataAberturaProposta")
    data_fim = item.get("dataEncerramentoProposta")

    # Fallback: busca datas no endpoint individual se vieram nulas
    if not data_inicio or not data_fim:
        log.info(f"   🔍 Buscando datas individuais para {cnpj}/{ano}/{seq}...")
        di, df = buscar_datas_individuais(cnpj, ano, seq)
        if di:
            data_inicio = di
        if df:
            data_fim = df
        time.sleep(0.3)

    # Itens
    itens_texto, itens_banco, valor_itens = buscar_itens_relevantes(cnpj, ano, seq)
    time.sleep(0.3)

    # Valor total
    valor_listagem = item.get("valorTotalEstimado")
    try:
        valor_total = valor_itens if valor_itens > 0 else (float(valor_listagem) if valor_listagem else 0.0)
    except Exception:
        valor_total = 0.0

    # Título
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
        # Campos auxiliares para montar o link (não persistidos)
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
    log.info("=" * 60)
    log.info("🔍 RADAR FARMAUSA v2 — Licitações Cannabis")
    log.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("❌ SUPABASE_URL ou SUPABASE_KEY não configurados!")
        sys.exit(1)
    if not TELEGRAM_BOT_TOKEN or not CHAT_ID:
        log.warning("⚠️ Telegram não configurado — alertas desativados")

    hoje = datetime.date.today()
    data_ini = hoje - datetime.timedelta(days=DIAS_RETROATIVOS)
    data_ini_str = data_ini.strftime("%Y%m%d")
    data_fim_str = hoje.strftime("%Y%m%d")

    log.info(f"📅 Período: {data_ini.strftime('%d/%m/%Y')} a {hoje.strftime('%d/%m/%Y')}")
    log.info(f"🔑 Keywords: {', '.join(KEYWORDS)}")
    log.info(f"📊 Modalidades: {MODALIDADES}")

    total_analisadas = 0
    total_encontradas = 0
    total_novas = 0

    for modalidade in MODALIDADES:
        log.info(f"--- Modalidade {modalidade} ---")
        contratacoes = buscar_todas_contratacoes(data_ini_str, data_fim_str, modalidade)
        log.info(f"   📄 {len(contratacoes)} contratações")
        total_analisadas += len(contratacoes)

        for item in contratacoes:
            if keyword_match(item.get("objetoCompra", "")):
                total_encontradas += 1
                log.info(f"   🎯 Match: {item.get('objetoCompra', '')[:80]}...")
                if processar_contratacao(item):
                    total_novas += 1

        time.sleep(0.5)

    log.info("=" * 60)
    log.info("📊 RESUMO:")
    log.info(f"   Analisadas:  {total_analisadas}")
    log.info(f"   Com cannabis: {total_encontradas}")
    log.info(f"   Novas (Telegram): {total_novas}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
