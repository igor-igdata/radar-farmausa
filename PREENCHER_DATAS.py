"""
PREENCHER DATAS - Radar FarmaUSA
=================================
Percorre TODOS os registros da tabela editais_pncp no Supabase
que estejam com data_inicio OU data_fim vazios,
busca as datas na API do PNCP e faz UPDATE.

O url_id tem o formato: /compras/{cnpj}/{ano}/{sequencial}
A API de consulta individual: GET /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}
Retorna: dataAberturaProposta, dataEncerramentoProposta
"""

import os
import requests
import time
import logging

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


def buscar_registros_sem_datas():
    """
    Busca todos os registros que têm data_inicio OU data_fim nulos.
    Pagina de 100 em 100 para não perder nenhum.
    """
    endpoint = f"{SUPABASE_URL}/rest/v1/{TABELA}"
    todos = []
    offset = 0
    limite = 100

    while True:
        headers = SUPABASE_HEADERS.copy()
        headers["Range"] = f"{offset}-{offset + limite - 1}"
        headers["Prefer"] = "count=exact"

        # Filtro: data_inicio IS NULL ou data_fim IS NULL
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
            # Range not satisfiable = não há mais dados
            break
        else:
            log.error(f"Erro ao buscar registros: {r.status_code} - {r.text[:300]}")
            break

    return todos


def buscar_todos_registros():
    """
    Alternativa: busca TODOS os registros (caso queira atualizar mesmo os que já têm data).
    """
    endpoint = f"{SUPABASE_URL}/rest/v1/{TABELA}"
    todos = []
    offset = 0
    limite = 100

    while True:
        headers = SUPABASE_HEADERS.copy()
        headers["Range"] = f"{offset}-{offset + limite - 1}"

        params = {
            "select": "url_id,data_inicio,data_fim",
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


def extrair_cnpj_ano_seq(url_id):
    """
    Extrai cnpj, ano e sequencial do url_id.
    Formato esperado: /compras/{cnpj}/{ano}/{sequencial}
    """
    partes = url_id.strip("/").split("/")
    # partes = ["compras", "cnpj", "ano", "sequencial"]
    if len(partes) >= 4 and partes[0] == "compras":
        return partes[1], partes[2], partes[3]
    else:
        log.warning(f"url_id com formato inesperado: {url_id}")
        return None, None, None


def buscar_datas_pncp(cnpj, ano, sequencial):
    """
    Consulta a API de detalhe da compra no PNCP.
    GET https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}
    Retorna (data_inicio, data_fim) ou (None, None).
    """
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}"

    try:
        r = requests.get(url, timeout=15)

        if r.status_code == 200:
            dados = r.json()
            data_inicio = dados.get("dataAberturaProposta")
            data_fim = dados.get("dataEncerramentoProposta")
            return data_inicio, data_fim

        elif r.status_code == 301:
            # Algumas rotas redirecionam - tentar seguir
            log.warning(f"  301 para {cnpj}/{ano}/{sequencial} - tentando consulta API")
            # Tenta a API de consulta pública como alternativa
            url_alt = f"https://pncp.gov.br/api/consulta/v1/contratacoes/{cnpj}/{ano}/{sequencial}"
            r2 = requests.get(url_alt, timeout=15)
            if r2.status_code == 200:
                dados = r2.json()
                return dados.get("dataAberturaProposta"), dados.get("dataEncerramentoProposta")
            return None, None

        elif r.status_code == 404:
            log.warning(f"  404 - Não encontrado: {cnpj}/{ano}/{sequencial}")
            return None, None
        else:
            log.warning(f"  HTTP {r.status_code} para {cnpj}/{ano}/{sequencial}")
            return None, None

    except requests.exceptions.Timeout:
        log.warning(f"  Timeout: {cnpj}/{ano}/{sequencial}")
        return None, None
    except Exception as e:
        log.error(f"  Erro: {cnpj}/{ano}/{sequencial} - {e}")
        return None, None


def atualizar_supabase(url_id, data_inicio, data_fim):
    """
    Faz PATCH no registro do Supabase para atualizar as datas.
    """
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

    # URL-encode do url_id para o filtro
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


def main():
    log.info("=" * 60)
    log.info("📅 PREENCHIMENTO DE DATAS - editais_pncp")
    log.info("=" * 60)

    # ========================================
    # OPÇÃO 1: Só os que faltam datas (padrão)
    # ========================================
    log.info("Buscando registros sem data_inicio ou data_fim...")
    registros = buscar_registros_sem_datas()

    # ========================================
    # OPÇÃO 2: Descomentar para rodar TODOS
    # ========================================
    # log.info("Buscando TODOS os registros...")
    # registros = buscar_todos_registros()

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

        # Buscar datas na API do PNCP
        data_inicio, data_fim = buscar_datas_pncp(cnpj, ano, seq)

        if data_inicio or data_fim:
            log.info(f"  ✅ Início: {data_inicio or 'N/A'} | Fim: {data_fim or 'N/A'}")
            if atualizar_supabase(url_id, data_inicio, data_fim):
                atualizados += 1
            else:
                erros += 1
        else:
            log.info(f"  ⚠️ Sem datas disponíveis na API")
            sem_dados += 1

        # Rate limiting: 0.4s entre requests (max ~150/min)
        time.sleep(0.4)

    log.info("=" * 60)
    log.info(f"📊 RESUMO:")
    log.info(f"   Total processados:  {len(registros)}")
    log.info(f"   ✅ Atualizados:     {atualizados}")
    log.info(f"   ⚠️ Sem dados API:   {sem_dados}")
    log.info(f"   ❌ Erros:           {erros}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
