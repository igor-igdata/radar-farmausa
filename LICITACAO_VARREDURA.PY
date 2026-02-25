"""
RADAR FARMAUSA - VARREDURA HISTÓRICA 30 DIAS
=============================================
Script para execução ÚNICA manual.

Objetivo: garantir que nenhuma licitação de cannabis dos últimos 30 dias
ficou fora do banco de dados antes de ativar o monitoramento diário.

Diferenças em relação ao LICITACAO.PY de produção:
- DIAS_RETROATIVOS = 30
- Pregão/Dispensa/Inexigibilidade: filtra por keyword nos ITENS também
- Concorrência/outros: filtra só pelo objetoCompra (itens raramente cadastrados)
- SEM envio de Telegram (só popula o banco silenciosamente)
- Pausa maior entre chamadas para não sobrecarregar a API do PNCP
- Timeout maior (30s) para aguentar a carga de 30 dias
- Log detalhado de progresso

Após rodar: verifique o banco no Supabase e então ative o LICITACAO.PY diário.
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
log = logging.getLogger("varredura")

# ================= CONFIGURAÇÕES =================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://clcaoyrqhkxirfekcxot.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99")

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

PNCP_API_BASE = "https://pncp.gov.br/api/consulta/v1"

DIAS_RETROATIVOS = 30
TAMANHO_PAGINA = 50

# Pausa entre chamadas (maior que produção para não sobrecarregar)
PAUSA_ENTRE_ITENS = 0.6
PAUSA_ENTRE_PAGINAS = 0.5
PAUSA_ENTRE_MODALIDADES = 1.0

# Pregão (6), Dispensa (8), Inexigibilidade (9):
#   itens geralmente cadastrados no PNCP → vale buscar itens de todas
# Concorrência (4,5), Pregão Presencial (7), Credenciamento (11), IRP (13):
#   itens raramente cadastrados → filtrar só pelo objetoCompra (evita centenas de 404)
MODALIDADES_COM_ITENS = [6, 8, 9]
MODALIDADES_SO_OBJETO = [4, 5, 7, 11, 13]
MODALIDADES = MODALIDADES_COM_ITENS + MODALIDADES_SO_OBJETO


# ================= UTILITÁRIOS =================

def keyword_match(texto):
    if not texto:
        return False
    texto_lower = str(texto).lower()
    return any(kw.lower() in texto_lower for kw in KEYWORDS)


def formatar_data_br(data_iso):
    if not data_iso:
        return "Não informada"
    try:
        dt = datetime.datetime.fromisoformat(data_iso.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(data_iso)


# ================= API PNCP =================

def buscar_datas_individuais(cnpj, ano, seq):
    """Busca datas no endpoint individual quando a listagem retorna null."""
    url = f"{PNCP_API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}"
    try:
        r = requests.get(url, headers=PNCP_HEADERS, timeout=30, allow_redirects=True)
        if r.status_code == 200:
            dados = r.json()
            if isinstance(dados, dict):
                return (
                    dados.get("dataAberturaProposta"),
                    dados.get("dataEncerramentoProposta")
                )
    except Exception as e:
        log.warning(f"  Erro datas individuais {cnpj}/{ano}/{seq}: {e}")
    return None, None


def buscar_itens(cnpj, ano, seq):
    """
    Busca TODOS os itens da contratação e filtra os relevantes por keyword.
    Retorna (itens_banco, valor_total, tem_cannabis)
    """
    url = f"{PNCP_API_BASE}/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
    itens_banco = []
    valor_total = 0.0

    try:
        r = requests.get(
            url,
            headers=PNCP_HEADERS,
            params={"pagina": 1, "tamanhoPagina": 500},
            timeout=30,
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
                vlr_total_item = item.get("valorTotalEstimado")

                if vlr_total_item:
                    try:
                        valor_total += float(vlr_total_item)
                    except Exception:
                        pass
                elif vlr_unit and qtd_raw:
                    try:
                        valor_total += float(vlr_unit) * float(qtd_raw)
                    except Exception:
                        pass

                itens_banco.append({
                    "numero_item": str(num),
                    "descricao": str(desc)[:500],
                    "quantidade": float(qtd_raw) if qtd_raw else 0.0,
                    "valor_unitario": float(vlr_unit) if vlr_unit else None,
                    "valor_total": float(vlr_total_item) if vlr_total_item else None,
                })

        elif r.status_code == 204:
            pass  # sem itens cadastrados
        elif r.status_code == 404:
            pass  # edital sem itens no PNCP (normal para algumas modalidades)
        else:
            log.warning(f"  Itens API {r.status_code} para {cnpj}/{ano}/{seq}")

    except Exception as e:
        log.warning(f"  Erro itens {cnpj}/{ano}/{seq}: {e}")

    tem_cannabis = len(itens_banco) > 0
    return itens_banco, valor_total, tem_cannabis


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
        r = requests.get(url, params=params, headers=PNCP_HEADERS, timeout=30)
        if r.status_code == 200:
            return r.json().get("data", [])
        elif r.status_code == 204:
            return []
        else:
            log.warning(f"API PNCP {r.status_code} mod={modalidade} pag={pagina}")
            return []
    except requests.exceptions.Timeout:
        log.warning(f"Timeout mod={modalidade} pag={pagina} — continuando")
        return []
    except Exception as e:
        log.error(f"Erro API PNCP: {e}")
        return []


def buscar_todas_contratacoes(data_ini_str, data_fim_str, modalidade):
    """Busca todas as páginas de uma modalidade no período."""
    todas = []
    pagina = 1
    while True:
        res = buscar_contratacoes_pagina(data_ini_str, data_fim_str, modalidade, pagina)
        if not res:
            break
        todas.extend(res)
        log.info(f"  pág {pagina}: {len(res)} contratações")
        if len(res) < TAMANHO_PAGINA:
            break
        pagina += 1
        time.sleep(PAUSA_ENTRE_PAGINAS)
        if pagina > 30:
            log.warning(f"  Limite de 30 páginas atingido mod={modalidade}")
            break
    return todas


# ================= SUPABASE =================

def salvar_supabase(dados_edital, dados_itens):
    """
    Upsert do edital. Retorna True se NOVO, False se já existia.
    Não envia Telegram — só popula o banco.
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

    if not is_new:
        return False  # já existe, pula

    # Insere edital novo
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
            log.error(f"ERRO upsert ({url_id}): {res.status_code} - {res.text[:100]}")
            return False
    except Exception as e:
        log.error(f"ERRO Supabase ({url_id}): {e}")
        return False

    # Insere itens
    if dados_itens:
        try:
            for item in dados_itens:
                item["edital_url_id"] = url_id
            requests.post(endpoint_itens, headers=SUPABASE_HEADERS, json=dados_itens, timeout=10)
        except Exception as e:
            log.warning(f"Erro ao inserir itens: {e}")

    return True


# ================= PROCESSAMENTO =================

def processar_contratacao(item, idx, total, buscar_itens_flag=True):
    """
    Processa uma contratação:
    - buscar_itens_flag=True  (Pregão, Dispensa): filtra por objeto OU por itens
    - buscar_itens_flag=False (Concorrência etc.): filtra só pelo objeto
    Sem Telegram — só popula o banco.
    """
    cnpj = item.get("orgaoEntidade", {}).get("cnpj", "")
    ano = item.get("anoCompra")
    seq = item.get("sequencialCompra")

    if not cnpj or not ano or not seq:
        return False

    objeto = item.get("objetoCompra", "")
    match_objeto = keyword_match(objeto)

    itens_banco = []
    valor_itens = 0.0
    match_itens = False

    if buscar_itens_flag:
        # Busca itens — pode ter cannabis mesmo sem mencionar no objeto
        itens_banco, valor_itens, match_itens = buscar_itens(cnpj, ano, seq)
        time.sleep(PAUSA_ENTRE_ITENS)
    else:
        # Modalidades que raramente cadastram itens: só checa objeto
        if not match_objeto:
            return False

    if not match_objeto and not match_itens:
        return False

    origem = "objeto" if match_objeto else "itens"
    log.info(f"  🎯 [{idx}/{total}] Match via {origem}: {objeto[:70]}...")

    url_id = f"/compras/{cnpj}/{ano}/{seq}"

    # Datas
    data_inicio = item.get("dataAberturaProposta")
    data_fim = item.get("dataEncerramentoProposta")
    if not data_inicio or not data_fim:
        di, df = buscar_datas_individuais(cnpj, ano, seq)
        if di:
            data_inicio = di
        if df:
            data_fim = df
        time.sleep(0.3)

    # Valor
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
    }

    novo = salvar_supabase(dados_edital, itens_banco)
    status = "✅ SALVO" if novo else "⏭ já existia"
    log.info(f"     {status} → {url_id}")
    return novo


# ================= MAIN =================

def main():
    log.info("=" * 60)
    log.info("🔍 RADAR FARMAUSA — VARREDURA HISTÓRICA 30 DIAS")
    log.info("   (execução única, sem Telegram, só banco)")
    log.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("❌ SUPABASE_URL ou SUPABASE_KEY não configurados!")
        sys.exit(1)

    hoje = datetime.date.today()
    data_ini = hoje - datetime.timedelta(days=DIAS_RETROATIVOS)
    data_ini_str = data_ini.strftime("%Y%m%d")
    data_fim_str = hoje.strftime("%Y%m%d")

    log.info(f"📅 Período: {data_ini.strftime('%d/%m/%Y')} a {hoje.strftime('%d/%m/%Y')}")
    log.info(f"🔑 Keywords: {', '.join(KEYWORDS)}")
    log.info(f"📈 Modalidades com busca de itens: {MODALIDADES_COM_ITENS}")
    log.info(f"📋 Modalidades só por objeto:      {MODALIDADES_SO_OBJETO}")
    log.info("=" * 60)

    total_analisadas = 0
    total_cannabis = 0
    total_novas = 0

    for modalidade in MODALIDADES:
        com_itens = modalidade in MODALIDADES_COM_ITENS
        estrategia = "objeto+itens" if com_itens else "só objeto"
        log.info(f"\n--- Modalidade {modalidade} ({estrategia}) ---")

        contratacoes = buscar_todas_contratacoes(data_ini_str, data_fim_str, modalidade)
        log.info(f"  📄 {len(contratacoes)} contratações encontradas")
        total_analisadas += len(contratacoes)

        for i, item in enumerate(contratacoes, 1):
            resultado = processar_contratacao(item, i, len(contratacoes), buscar_itens_flag=com_itens)
            if resultado is not False:
                total_cannabis += 1
                if resultado:
                    total_novas += 1

        time.sleep(PAUSA_ENTRE_MODALIDADES)

    log.info("\n" + "=" * 60)
    log.info("📊 RESUMO FINAL:")
    log.info(f"   Contratações analisadas: {total_analisadas}")
    log.info(f"   Com cannabis (obj+itens): {total_cannabis}")
    log.info(f"   Novas salvas no banco:    {total_novas}")
    log.info("=" * 60)
    log.info("✅ Varredura concluída. Verifique o banco no Supabase.")
    log.info("   Próximo passo: ativar LICITACAO.PY com DIAS_RETROATIVOS=1")


if __name__ == "__main__":
    main()
