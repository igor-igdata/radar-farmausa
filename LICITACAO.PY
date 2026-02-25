import requests
from datetime import datetime

# --- CONFIGURAÇÕES DE CONEXÃO ---
SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
SUPABASE_KEY = "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99"

# Headers cruciais para o UPSERT (Merge)
HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" 
}

def executar_robo():
    # 1. Busca licitações de Cannabis no PNCP
    url_pncp = "https://pncp.gov.br/api/pncp/v1/editais?pagina=1&termo=cannabis"
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Acessando PNCP...")
    response = requests.get(url_pncp, timeout=20)
    
    if response.status_code != 200:
        print(f"❌ Erro ao acessar PNCP: {response.status_code}")
        return

    licitacoes = response.json().get('data', [])
    print(f"✅ {len(licitacoes)} licitações encontradas. Iniciando atualização do banco...")

    for licitacao in licitacoes:
        # Extração dos IDs para montar o url_id (nossa chave única)
        cnpj = licitacao.get('orgaoEntidade', {}).get('cnpj')
        ano = licitacao.get('anoEdital')
        seq = licitacao.get('sequencialEdital')
        
        # Monta o url_id exatamente como está no seu banco
        url_id = f"/compras/{cnpj}/{ano}/{seq}"
        
        # Prepara o pacote de dados com as novas colunas de data
        dados_supabase = {
            "url_id": url_id, 
            "titulo": licitacao.get('numeroEdital'),
            "orgao": licitacao.get('orgaoEntidade', {}).get('razaoSocial'),
            "uf": licitacao.get('uf'),
            "modalidade": licitacao.get('modalidadeNome'),
            "data_publicacao": licitacao.get('dataPublicacaoPncp'),
            "data_inicio": licitacao.get('dataAberturaPropostas'),
            "data_fim": licitacao.get('dataEncerramentoPropostas')
        }

        # 2. Envio para o Supabase
        # AQUI ESTÁ A MÁGICA: ?on_conflict=url_id avisa o banco exatamente onde comparar
        url_post = f"{SUPABASE_URL}/rest/v1/editais_pncp?on_conflict=url_id"
        
        res = requests.post(
            url_post, 
            headers=HEADERS_SUPABASE, 
            json=dados_supabase
        )

        if res.status_code in [200, 201, 204]:
            print(f"✔️ Sucesso (Upsert): {url_id}")
        else:
            print(f"⚠️ Erro em {url_id}: Status {res.status_code} - {res.text}")

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Banco de dados atualizado com sucesso!")

if __name__ == "__main__":
    executar_robo()
