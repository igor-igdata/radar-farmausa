import requests
import datetime

# ================= CONFIGURAÇÕES =================
TELEGRAM_BOT_TOKEN = "8388155318:AAGrSb4FwLvAS51PZG4tmnapkM2V7p0lTYk"
CHAT_ID = "-5236299203"

SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
SUPABASE_KEY = "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal" 
}

KEYWORDS = ["canabidiol", "cannabis", "CBD"]
# =================================================

def formatar_data_br(data_iso):
    if not data_iso: return "Não informada"
    try:
        dt = datetime.datetime.fromisoformat(data_iso.replace('Z', ''))
        return dt.strftime('%d/%m/%Y %H:%M')
    except: return data_iso

def buscar_detalhes_datas(cnpj, ano, sequencial):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("dataAberturaProposta"), data.get("dataEncerramentoProposta")
        else:
            print(f"⚠️ Erro ao buscar datas no PNCP: Status {r.status_code}")
    except Exception as e:
        print(f"⚠️ Erro de conexão com PNCP: {e}")
    return None, None

def check_and_save_supabase(dados_edital, dados_itens):
    url_id = dados_edital["url_id"]
    endpoint_editais = f"{SUPABASE_URL}/rest/v1/editais_pncp"
    endpoint_itens = f"{SUPABASE_URL}/rest/v1/itens_pncp"
    
    is_new = True
    try:
        check_response = requests.get(
            f"{endpoint_editais}?url_id=eq.{url_id}&select=url_id",
            headers=SUPABASE_HEADERS,
            timeout=10
        )
        if check_response.status_code == 200 and len(check_response.json()) > 0:
            is_new = False 
    except Exception as e:
        print(f"Aviso GET: {e}")
    
    # UPSERT FORÇADO COM LOG DE ERRO
    headers_upsert = SUPABASE_HEADERS.copy()
    headers_upsert["Prefer"] = "resolution=merge-duplicates"
    
    res = requests.post(
        f"{endpoint_editais}?on_conflict=url_id",
        headers=headers_upsert,
        json=dados_edital,
        timeout=10
    )
    
    # RAIO-X: Se o Supabase recusar, vai gritar o erro exato aqui!
    if res.status_code not in [200, 201, 204]:
        print(f"🚨 ERRO SUPABASE ({url_id}): {res.status_code} - {res.text}")
        return False
    else:
        print(f"✅ Upsert OK ({url_id}) - Início: {dados_edital.get('data_inicio')} | Fim: {dados_edital.get('data_fim')}")
    
    if is_new:
        if dados_itens:
            for item in dados_itens:
                item["edital_url_id"] = url_id
            requests.post(endpoint_itens, headers=SUPABASE_HEADERS, json=dados_itens, timeout=10)
        return True 
        
    return False

def classificar_modalidade(modalidade):
    mod_lower = str(modalidade).lower()
    if "dispensa" in mod_lower or "inexigibilidade" in mod_lower:
        return "⚖️ <b>JUDICIALIZAÇÃO / COMPRA DIRETA</b>"
    return "📈 <b>GRANDE VOLUME / PREGÃO</b>"

def buscar_itens_relevantes(cnpj, ano, sequencial):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
    apresentacoes_texto, apresentacoes_banco = [], []
    valor_total = 0.0
    try:
        r = requests.get(url, params={"pagina": 1, "tamanhoPagina": 500}, timeout=15)
        if r.status_code == 200:
            itens = r.json() if isinstance(r.json(), list) else r.json().get("items", r.json().get("data", []))
            for item in itens:
                desc = item.get("descricao", item.get("materialOuServicoNome", ""))
                if any(kw.lower() in str(desc).lower() for kw in KEYWORDS):
                    num = item.get("numeroItem", "-")
                    qtd = item.get("quantidade", 0)
                    vlr = item.get("valorTotalEstimado")
                    if vlr: valor_total += float(vlr)
                    apresentacoes_texto.append(f"Item {num}: {desc}\n↳ Qtd: {qtd}")
                    apresentacoes_banco.append({
                        "numero_item": str(num), "descricao": str(desc),
                        "quantidade": float(qtd) if qtd else 0.0,
                        "valor_unitario": float(item.get("valorUnitarioEstimado")) if item.get("valorUnitarioEstimado") else None,
                        "valor_total": float(vlr) if vlr else None
                    })
    except: pass
    return apresentacoes_texto, apresentacoes_banco, valor_total

def enviar_telegram(edital, apresentacoes_texto, tag):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    dt_inicio = formatar_data_br(edital.get("data_inicio"))
    dt_fim = formatar_data_br(edital.get("data_fim"))
    
    msg = f"""🏛️ <b>NOVA LICITAÇÃO - CANNABIS</b>
{tag}
📋 <b>Título:</b> {edital.get('titulo')}
🏢 <b>Órgão:</b> {edital.get('orgao')}
⏳ <b>CRONOGRAMA:</b>
🟢 <b>Início:</b> {dt_inicio}
🔴 <b>FIM:</b> {dt_fim}
🔗 <a href="https://pncp.gov.br/app/editais{edital.get('url_id')}">Acessar no PNCP</a>
<i>Alerta automático - FarmaUSA Life</i>"""
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"})

def buscar_editais():
    print("Iniciando busca (Modo Histórico e Debug Ativado)...")
    for keyword in KEYWORDS:
        api_url = f"https://pncp.gov.br/api/search/?q={keyword}&tipos_documento=edital&ordenacao=-data&pagina=1&tam_pagina=20"
        try:
            r = requests.get(api_url, timeout=10)
            if r.status_code != 200: continue
            
            for item in r.json().get("items", []):
                url_id = item.get("item_url")
                cnpj = item.get('orgao_cnpj')
                ano = item.get('ano')
                seq = item.get('numero_sequencial')
                
                apres_texto, apres_banco, valor = buscar_itens_relevantes(cnpj, ano, seq)
                data_inicio, data_fim = buscar_detalhes_datas(cnpj, ano, seq)
                
                dados_edital = {
                    "url_id": url_id, "titulo": item.get("title"),
                    "orgao": item.get("orgao_nome"), "uf": item.get("uf"),
                    "modalidade": item.get("modalidade_licitacao_nome"),
                    "data_publicacao": item.get("data_publicacao_pncp", ""),
                    "valor_total_estimado": valor,
                    "data_inicio": data_inicio, "data_fim": data_fim
                }
                
                if check_and_save_supabase(dados_edital, apres_banco):
                    tag = classificar_modalidade(item.get("modalidade_licitacao_nome"))
                    enviar_telegram(dados_edital, apres_texto, tag)
                    print(f"🚀 Enviado Telegram: {item.get('title')}")
        except Exception as e: print(f"Erro geral: {e}")

if __name__ == "__main__":
    buscar_editais()
