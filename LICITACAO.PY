import requests
from datetime import datetime

# --- CONFIGURAÇÕES ---
SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
SUPABASE_KEY = "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99"

# Headers para o UPSERT (Merge)
HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" 
}

def executar_robo():
    # 1. Busca no PNCP (Mantendo sua lógica de busca por Cannabis)
    url_pncp = "https://pncp.gov.br/api/pncp/v1/editais?pagina=1&termo=cannabis"
    response = requests.get(url_pncp)
    
    if response.status_code != 200:
        print("Erro ao acessar PNCP")
        return

    licitacoes = response.json().get('data', [])
    print(f"Processando {len(licitacoes)} licitações...")

    for licitacao in licitacoes:
        # Mapeando os campos conforme seu JSON
        url_id = f"/compras/{licitacao.get('orgaoEntidade', {}).get('cnpj')}/{licitacao.get('anoEdital')}/{licitacao.get('sequencialEdital')}"
        
        dados_supabase = {
            "url_id": url_id, # Nossa chave única para o Upsert
            "titulo": licitacao.get('numeroEdital'),
            "orgao": licitacao.get('orgaoEntidade', {}).get('razaoSocial'),
            "uf": licitacao.get('uf'),
            "modalidade": licitacao.get('modalidadeNome'),
            "data_publicacao": licitacao.get('dataPublicacaoPncp'),
            "data_inicio": licitacao.get('dataAberturaPropostas'),
            "data_fim": licitacao.get('dataEncerramentoPropostas')
        }

        # Faz o Upsert no Supabase
        res = requests.post(
            f"{SUPABASE_URL}/rest/v1/editais_pncp", 
            headers=HEADERS_SUPABASE, 
            json=dados_supabase
        )

        if res.status_code in [200, 201]:
            print(f"✅ Atualizado: {url_id}")
        else:
            print(f"❌ Erro em {url_id}: {res.status_code}")

if __name__ == "__main__":
    executar_robo()
