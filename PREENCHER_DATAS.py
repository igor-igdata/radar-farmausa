"""
PREENCHER DATAS - Radar FarmaUSA (v3 - ENDPOINT CORRIGIDO)
===========================================================
Percorre registros da tabela editais_pncp no Supabase
que estejam com data_inicio OU data_fim vazios,
busca as datas na API do PNCP e faz UPDATE.

CORREÇÃO v3:
- Endpoint correto: /api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}
  (o v2 usava /api/pncp/v1/... que retorna 301, e /api/consulta/v1/contratacoes/...
   que retorna 404 — ambos inválidos)
- Link front-end correto: https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}
- Fallback: endpoint de itens para extrair datas quando o principal não retorna
"""

import os
import requests
import time
import logging
import json

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("preencher_datas")

# ================= CONFIGURAÇÕES =================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://clcaoyrqhkxirfekcxot.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

TABELA = "editais_pncp"

PNCP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


# ================= SUPABASE =================

def buscar_registros_sem_datas():
    endpoint = f"{SUPABASE_URL}/rest/v1/{TABELA}"
    todos = []
    offset = 0
    limite = 100

    while True:
        headers = SUPABASE_HEADERS.copy()
        headers["Range"] = f"{offset}-{offset + limite - 1}"
        headers["Prefer"] = "count=exact"

        params = {
            "select": "url_id,data_inicio,data_fim",
            "or": "(data_inicio.is.null,data_fim.is.null)",
            "order": "data_publicacao.asc",
        }

        r = requests.get(endpoint, headers=headers, params=params, timeout=15)

        if r.status_code == 200:
            dados = r.json()
            if not dados:
                break
            todos.extend(dados)
            log.info(f"  Carregados {len(todos)} registros...")
            if len(dados) < limite:
                break
            offset += limite
        elif r.status_code == 416:
            break
        else:
            log.error(f"Erro ao buscar registros: {r.status_code} - {r.text[:300]}")
            break

    return todos


def atualizar_supabase(url_id, data_inicio, data_fim):
    endpoint = f"{SUPABASE_URL}/rest/v1/{TABELA}"

    payload = {}
    if data_inicio:
        payload["data_inicio"] = data_inicio
    if data_fim:
        payload["data_fim"] = data_fim

    if not payload:
        return False

    headers = SUPABASE_HEADERS.copy()
    headers["Prefer"] = "return=minimal"

    r = requests.patch(
        f"{endpoint}?url_id=eq.{requests.utils.quote(url_id, safe='')}",
        headers=headers,
        json=payload,
        timeout=10
    )

    if r.status_code in [200, 204]:
        return True
    else:
        log.error(f"  Erro UPDATE Supabase: {r.status_code} - {r.text[:200]}")
        return False


# ================= PNCP =================

def extrair_cnpj_ano_seq(url_id):
    """
    Extrai cnpj, ano e sequencial do url_id armazenado no Supabase.
    Suporta formatos:
      /compras/42498600000171/2026/670
      compras/42498600000171/2026/670
    """
    partes = url_id.strip("/").split("/")
    if len(partes) >= 4 and partes[0] == "compras":
        return partes[1], partes[2], partes[3]
    else:
        log.warning(f"url_id com formato inesperado: {url_id}")
        return None, None, None


def extrair_datas_do_json(dados):
    """
    Tenta extrair datas de múltiplos campos possíveis da resposta da API.
    """
    campos_inicio = [
        "dataAberturaProposta",
        "dataAberturaPropostas",
        "dataInicioProposta",
        "dataInicioRecebimentoPropostas",
        "dataInicio",
        "dataInicioVigencia",
        "dataAbertura",
        "dataPublicacaoPncp",
    ]

    campos_fim = [
        "dataEncerramentoProposta",
        "dataEncerramentoPropostas",
        "dataFimProposta",
        "dataFimRecebimentoPropostas",
        "dataFim",
        "dataFimVigencia",
        "dataEncerramento",
    ]

    data_inicio = None
    data_fim = None

    for campo in campos_inicio:
        val = dados.get(campo)
        if val:
            data_inicio = val
            break

    for campo in campos_fim:
        val = dados.get(campo)
        if val:
            data_fim = val
            break

    return data_inicio, data_fim


def buscar_datas_pncp(cnpj, ano, seq, debug=False):
    """
    Consulta a API do PNCP no endpoint correto.

    Endpoint oficial (confirmado):
      GET https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}

    Link front-end correspondente:
      https://pncp.gov.br/app/editais/{cnpj}/{ano}/{sequencial}

    Fallback: endpoint de itens da contratação, que às vezes retorna mais campos.
    """
    # ── Endpoint principal (CORRETO) ──────────────────────────────────────────
    endpoints = [
        f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}",
        # Fallback: alguns editais usam a rota de contratações com estrutura diferente
        f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens?pagina=1&tamanhoPagina=1",
    ]

    for i, url in enumerate(endpoints):
        try:
            r = requests.get(
                url,
                headers=PNCP_HEADERS,
                timeout=15,
                allow_redirects=True
            )

            if debug:
                log.info(f"  [DEBUG] Endpoint {i+1}: {url}")
                log.info(f"  [DEBUG] Status: {r.status_code}")
                if r.status_code == 200:
                    try:
                        dados = r.json()
                        if isinstance(dados, dict):
                            data_fields = {k: v for k, v in dados.items() if 'ata' in k.lower()}
                            log.info(f"  [DEBUG] Campos data: {json.dumps(data_fields, default=str)[:500]}")
                            log.info(f"  [DEBUG] Todas as chaves: {list(dados.keys())}")
                        elif isinstance(dados, list):
                            log.info(f"  [DEBUG] Lista com {len(dados)} itens")
                            if dados:
                                log.info(f"  [DEBUG] Chaves[0]: {list(dados[0].keys())}")
                    except Exception:
                        log.info(f"  [DEBUG] Body (não-JSON): {r.text[:300]}")
                else:
                    log.info(f"  [DEBUG] Body: {r.text[:300]}")

            if r.status_code == 200:
                try:
                    dados = r.json()
                except Exception:
                    continue

                # Resposta principal é um dict
                if isinstance(dados, dict):
                    data_inicio, data_fim = extrair_datas_do_json(dados)
                    if data_inicio or data_fim:
                        return data_inicio, data_fim

                # Fallback de itens retorna lista — pega datas do primeiro item
                elif isinstance(dados, list) and dados:
                    data_inicio, data_fim = extrair_datas_do_json(dados[0])
                    if data_inicio or data_fim:
                        return data_inicio, data_fim

        except requests.exceptions.Timeout:
            if debug:
                log.info(f"  [DEBUG] Timeout no endpoint {i+1}")
            continue
        except Exception as e:
            if debug:
                log.info(f"  [DEBUG] Erro endpoint {i+1}: {e}")
            continue

        time.sleep(0.3)

    return None, None


def montar_link_pncp(cnpj, ano, seq):
    """Retorna o link front-end correto do PNCP para o edital."""
    return f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"


# ================= MAIN =================

def main():
    log.info("=" * 60)
    log.info("📅 PREENCHIMENTO DE DATAS - editais_pncp (v3)")
    log.info("=" * 60)

    log.info("Buscando registros sem data_inicio ou data_fim...")
    registros = buscar_registros_sem_datas()

    if not registros:
        log.info("✅ Todos os registros já possuem datas! Nada a fazer.")
        return

    log.info(f"📋 {len(registros)} registros para processar")
    log.info("-" * 60)

    atualizados = 0
    sem_dados = 0
    erros = 0

    for i, reg in enumerate(registros, 1):
        url_id = reg["url_id"]
        log.info(f"[{i}/{len(registros)}] {url_id}")

        cnpj, ano, seq = extrair_cnpj_ano_seq(url_id)
        if not cnpj:
            erros += 1
            continue

        # Debug detalhado nos primeiros 3 registros para validar campos retornados
        debug = (i <= 3)

        data_inicio, data_fim = buscar_datas_pncp(cnpj, ano, seq, debug=debug)

        if data_inicio or data_fim:
            link = montar_link_pncp(cnpj, ano, seq)
            log.info(f"  ✅ Início: {data_inicio or 'N/A'} | Fim: {data_fim or 'N/A'}")
            log.info(f"  🔗 {link}")
            if atualizar_supabase(url_id, data_inicio, data_fim):
                atualizados += 1
            else:
                erros += 1
        else:
            log.info(f"  ⚠️ Sem datas disponíveis na API")
            sem_dados += 1

        time.sleep(0.4)

    log.info("=" * 60)
    log.info("📊 RESUMO:")
    log.info(f"   Total processados:  {len(registros)}")
    log.info(f"   ✅ Atualizados:     {atualizados}")
    log.info(f"   ⚠️ Sem dados API:   {sem_dados}")
    log.info(f"   ❌ Erros:           {erros}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
