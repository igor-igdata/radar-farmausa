import requests
import datetime

# ================= CONFIGURAÇÕES =================
TELEGRAM_BOT_TOKEN = "8388155318:AAGrSb4FwLvAS51PZG4tmnapkM2V7p0lTYk"
CHAT_ID = "-5236299203"

# Chaves do Supabase
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
    """Formata a data ISO para o padrão Brasileiro no Telegram"""
    if not data_iso: return "Não informada"
    try:
        dt = datetime.datetime.fromisoformat(data_iso.replace('Z', ''))
        return dt.strftime('%d/%m/%Y %H:%M')
    except: return data_iso

def buscar_detalhes_datas(cnpj, ano, sequencial):
    """Busca as datas exatas de abertura e encerramento direto na fonte"""
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("dataAberturaProposta"), data.get("dataEncerramentoProposta")
    except:
        pass
    return None, None

def check_and_save_supabase(dados_edital, dados_itens):
    """Salva Edital novo ou Atualiza (Upsert) um existente sem gerar spam"""
    url_id = dados_edital["url_id"]
    endpoint_editais = f"{SUPABASE_URL}/rest/v1/editais_pncp"
    endpoint_itens = f"{SUPABASE_URL}/rest/v1/itens_pncp"
    
    is_new = True
    # 1. Verifica se já existe
    try:
        check_response = requests.get(
            f"{endpoint_editais}?url_id=eq.{url_id}&select=url_id",
            headers=SUPABASE_HEADERS,
            timeout=10
        )
        if check_response.status_code == 200 and len(check_response.json()) > 0:
            is_new = False # Já existe! Não vamos mandar pro Telegram.
    except Exception as e:
        print(f"Aviso: Erro ao verificar banco de dados: {e}")
    
    # 2. Faz o UPSERT do Edital de qualquer forma (Para preencher as datas vazias)
    headers_upsert = SUPABASE_HEADERS.copy()
    headers_upsert["Prefer"] = "resolution=merge-duplicates"
    
    requests.post(
        f"{endpoint_editais}?on_conflict=url_id",
        headers=headers_upsert,
        json=dados_edital,
        timeout=10
    )
    
    # 3. Só insere itens na outra tabela e libera o Telegram se for um edital INÉDITO
    if is_new:
        if dados_itens:
            for item in dados_itens:
                item["edital_url_id"] = url_id
            requests.post(endpoint_itens, headers=SUPABASE_HEADERS, json=dados_itens, timeout=10)
        return True # Libera o envio da mensagem
        
    return False # Bloqueia a mensagem (foi só uma atualização de banco)

def classificar_modalidade(modalidade):
    mod_lower = str(modalidade).lower()
    if "dispensa" in mod_lower or "inexigibilidade" in mod_lower:
        return "⚖️ <b>JUDICIALIZAÇÃO / COMPRA DIRETA</b> (Ação Rápida)"
    elif "pregão" in mod_lower or "concorrência" in mod_lower:
        return "📈 <b>GRANDE VOLUME / PREGÃO</b> (Planejamento)"
    return f"📂 <b>{modalidade}</b>"

def buscar_itens_relevantes(cnpj, ano, sequencial):
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
    apresentacoes_texto = []
    apresentacoes_banco = [] 
    valor_total_edital = 0.0
    
    try:
        params = {"pagina": 1, "tamanhoPagina": 500}
        response = requests.get(url, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            itens = data if isinstance(data, list) else data.get("items", data.get("data", []))
            
            for item in itens:
                descricao = item.get("descricao", item.get("materialOuServicoNome", ""))
                
                if any(kw.lower() in str(descricao).lower() for kw in KEYWORDS):
                    num = item.get("numeroItem", "-")
                    qtd = item.get("quantidade", 0)
                    vlr_unit = item.get("valorUnitarioEstimado")
                    vlr_total = item.get("valorTotalEstimado")
                    
                    str_unit = f"R$ {vlr_unit:,.2f}" if vlr_unit else "Sigiloso/Não inf."
                    str_total = f"R$ {vlr_total:,.2f}" if vlr_total else "Sigiloso/Não inf."
                    
                    if vlr_total: valor_total_edital += float(vlr_total)
                    
                    texto_item = f"Item {num}: {descricao}\n   ↳ Qtd: {qtd} | V. Unit: {str_unit} | V. Total: {str_total}"
                    apresentacoes_texto.append(texto_item)
                    
                    apresentacoes_banco.append({
                        "numero_item": str(num),
                        "descricao": str(descricao),
                        "quantidade": float(qtd) if qtd else 0.0,
                        "valor_unitario": float(vlr_unit) if vlr_unit else None,
                        "valor_total": float(vlr_total) if vlr_total else None
                    })
    except Exception as e:
        print(f"Erro ao buscar itens: {e}")
        
    return apresentacoes_texto, apresentacoes_banco, valor_total_edital

def enviar_telegram(edital, apresentacoes_texto, tag_estrategica):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data_pub = edital.get("data_publicacao", "")[:16].replace("T", " ")
    
    # Formata as datas para o Telegram
    dt_inicio = formatar_data_br(edital.get("data_inicio"))
    dt_fim = formatar_data_br(edital.get("data_fim"))
    
    itens_texto = ""
    if apresentacoes_texto:
        itens_texto = "\n\n📦 <b>Itens e Valores:</b>\n"
        for idx, ap in enumerate(apresentacoes_texto):
            if idx < 10: itens_texto += f"• {ap}\n"
            elif idx == 10:
                itens_texto += f"• ... e mais {len(apresentacoes_texto) - 10} itens."
                break
    else:
         itens_texto = "\n\n📦 <b>Apresentações / Itens:</b> Listados apenas nos anexos."

    mensagem = f"""🏛️ <b>NOVA LICITAÇÃO - CANNABIS</b>
{tag_estrategica}

📋 <b>Título:</b> {edital.get('titulo')}
🏢 <b>Órgão:</b> {edital.get('orgao')}
📍 <b>Local:</b> {edital.get('uf')}

⏳ <b>CRONOGRAMA DE DISPUTA:</b>
🟢 <b>Início:</b> {dt_inicio}
🔴 <b>PRAZO FINAL:</b> {dt_fim}
{itens_texto}

📌 <b>Situação:</b> Divulgada no PNCP
📅 <b>Publicação:</b> {data_pub}

🔗 <a href="https://pncp.gov.br/app/editais{edital.get('url_id')}">Acessar no PNCP</a>

<i>Alerta automático - FarmaUSA Life</i>"""

    requests.post(url, json={"chat_id": CHAT_ID, "text": mensagem, "parse_mode": "HTML", "disable_web_page_preview": True})

def buscar_editais():
    print("Iniciando busca (Modo Histórico Recente Ativado)...")

    for keyword in KEYWORDS:
        print(f"Buscando por: {keyword}...")
        api_url = f"https://pncp.gov.br/api/search/?q={keyword}&tipos_documento=edital&ordenacao=-data&pagina=1&tam_pagina=20"
        
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get("items", []):
                # Desativei o filtro de datas (if any(data_valida...)) para ele atualizar o banco todo!
                url_id = item.get("item_url")
                cnpj = item.get('orgao_cnpj')
                ano = item.get('ano')
                sequencial = item.get('numero_sequencial')
                
                # Busca itens e as DATAS oficiais
                apres_texto, apres_banco, valor_total_calc = buscar_itens_relevantes(cnpj, ano, sequencial)
                data_inicio, data_fim = buscar_detalhes_datas(cnpj, ano, sequencial)
                
                dados_edital = {
                    "url_id": url_id,
                    "titulo": item.get("title"),
                    "orgao": item.get("orgao_nome"),
                    "uf": item.get("uf"),
                    "modalidade": item.get("modalidade_licitacao_nome"),
                    "data_publicacao": item.get("data_publicacao_pncp", ""),
                    "valor_total_estimado": valor_total_calc,
                    "data_inicio": data_inicio,
                    "data_fim": data_fim
                }
                
                # Se retornar True, é porque era novo e precisa avisar o comercial
                if check_and_save_supabase(dados_edital, apres_banco):
                    tag = classificar_modalidade(item.get("modalidade_licitacao_nome"))
                    enviar_telegram(dados_edital, apres_texto, tag)
                    print(f"Novo edital salvo e enviado: {item.get('title')}")
                else:
                    print(f"Atualizado no banco: {item.get('title')}")
                        
        except Exception as e:
            print(f"Erro na execução: {e}")

if __name__ == "__main__":
    buscar_editais()
