"""
PREENCHER DATAS - Radar FarmaUSA (v2 - CORRIGIDO)
===================================================
Percorre registros da tabela editais_pncp no Supabase
que estejam com data_inicio OU data_fim vazios,
busca as datas na API do PNCP e faz UPDATE.

CORREÇÃO v2: 
- Segue redirects corretamente (allow_redirects=True)
- Tenta múltiplos endpoints da API do PNCP
- Busca campos alternativos para datas
- Log de debug nos primeiros registros para diagnóstico
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

# Headers para simular navegador (PNCP pode bloquear requests sem User-Agent)
PNCP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


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


def extrair_cnpj_ano_seq(url_id):
    partes = url_id.strip("/").split("/")
    if len(partes) >= 4 and partes[0] == "compras":
        return partes[1], partes[2], partes[3]
    else:
        log.warning(f"url_id com formato inesperado: {url_id}")
        return None, None, None


def extrair_datas_do_json(dados):
    """
    Tenta extrair datas de múltiplos campos possíveis.
    """
    campos_inicio = [
        "dataAberturaProposta",
        "dataAberturaPropostas", 
        "dataInicioProposta",
        "dataInicioRecebimentoPropostas",
        "dataInicio",
        "dataInicioVigencia",
        "dataAbertura",
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


def buscar_datas_pncp(cnpj, ano, sequencial, debug=False):
    """
    Consulta a API do PNCP tentando múltiplos endpoints.
    """
    endpoints = [
        f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}",
        f"https://pncp.gov.br/api/consulta/v1/contratacoes/{cnpj}/{ano}/{sequencial}",
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
                log.info(f"  [DEBUG] Status: {r.status_code} | Final URL: {r.url}")
                if r.history:
                    log.info(f"  [DEBUG] Redirects: {len(r.history)} hops")
                if r.status_code == 200:
                    try:
                        dados = r.json()
                        if isinstance(dados, dict):
                            # Mostra campos que contêm 'data' no nome
                            data_fields = {k: v for k, v in dados.items() if 'ata' in k.lower()}
                            log.info(f"  [DEBUG] Campos com 'data': {json.dumps(data_fields, default=str)[:800]}")
                            # Mostra todas as chaves
                            log.info(f"  [DEBUG] Todas as chaves: {list(dados.keys())[:30]}")
                        elif isinstance(dados, list):
                            log.info(f"  [DEBUG] Retornou lista com {len(dados)} itens")
                            if dados:
                                data_fields = {k: v for k, v in dados[0].items() if 'ata' in k.lower()}
                                log.info(f"  [DEBUG] Campos[0] com 'data': {json.dumps(data_fields, default=str)[:800]}")
                        else:
                            log.info(f"  [DEBUG] Tipo inesperado: {type(dados).__name__}")
                    except Exception as e:
                        log.info(f"  [DEBUG] Não é JSON: {r.text[:200]}")
                else:
                    # Mostra o que veio mesmo quando não é 200
                    content_type = r.headers.get('Content-Type', '')
                    log.info(f"  [DEBUG] Content-Type: {content_type}")
                    if 'json' in content_type or 'text' in content_type:
                        log.info(f"  [DEBUG] Body: {r.text[:300]}")
            
            if r.status_code == 200:
                try:
                    dados = r.json()
                except:
                    continue
                
                if isinstance(dados, dict):
                    data_inicio, data_fim = extrair_datas_do_json(dados)
                    if data_inicio or data_fim:
                        return data_inicio, data_fim
                elif isinstance(dados, list) and len(dados) > 0:
                    data_inicio, data_fim = extrair_datas_do_json(dados[0])
                    if data_inicio or data_fim:
                        return data_inicio, data_fim
            
        except requests.exceptions.Timeout:
            if debug:
                log.info(f"  [DEBUG] Timeout endpoint {i+1}")
            continue
        except Exception as e:
            if debug:
                log.info(f"  [DEBUG] Erro endpoint {i+1}: {e}")
            continue
        
        time.sleep(0.2)
    
    return None, None


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


def main():
    log.info("=" * 60)
    log.info("📅 PREENCHIMENTO DE DATAS - editais_pncp (v2)")
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

        # Debug detalhado nos primeiros 3 registros
        debug = (i <= 3)

        data_inicio, data_fim = buscar_datas_pncp(cnpj, ano, seq, debug=debug)

        if data_inicio or data_fim:
            log.info(f"  ✅ Início: {data_inicio or 'N/A'} | Fim: {data_fim or 'N/A'}")
            if atualizar_supabase(url_id, data_inicio, data_fim):
                atualizados += 1
            else:
                erros += 1
        else:
            log.info(f"  ⚠️ Sem datas disponíveis na API")
            sem_dados += 1

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
