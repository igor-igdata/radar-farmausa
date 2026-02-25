import streamlit as st
import requests
import pandas as pd
from datetime import datetime

# 1. Configuração da Página
st.set_page_config(page_title="Radar FarmaUSA - PNCP", page_icon="🌿", layout="wide")

# 2. Credenciais (Seu Supabase)
SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
SUPABASE_KEY = "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

@st.cache_data(ttl=60) # Atualiza a cada 1 minuto se houver F5
def carregar_dados():
    try:
        # Puxa os dados da sua tabela específica
        url = f"{SUPABASE_URL}/rest/v1/editais_pncp?select=*"
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        df = pd.DataFrame(res.json())
        
        if df.empty:
            return pd.DataFrame()

        # Tratamento das Datas (Crucial para o comercial)
        # Convertemos as ISO Strings que o PNCP envia para objetos de data do Python
        df['dt_fim_obj'] = pd.to_datetime(df['data_fim'], errors='coerce')
        
        # Criamos versões bonitas para ler na tabela
        df['Início Propostas'] = pd.to_datetime(df['data_inicio'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
        df['Fim Propostas'] = df['dt_fim_obj'].dt.strftime('%d/%m/%Y %H:%M')
        
        return df
    except Exception as e:
        st.error(f"Erro ao conectar com o banco: {e}")
        return pd.DataFrame()

# --- TELA PRINCIPAL ---
st.title("🏛️ Radar de Licitações - FarmaUSA")

df = carregar_dados()

if not df.empty:
    # Lógica de Status (Coração do Dashboard)
    hoje = datetime.now()
    
    def definir_status(row):
        if pd.isna(row['dt_fim_obj']): 
            return "⚪ Aguardando Robô" # Aparece antes de você rodar o LICITACAO.PY novo
        
        # Calcula a diferença em horas
        diff_horas = (row['dt_fim_obj'] - hoje).total_seconds() / 3600
        
        if diff_horas < 0:
            return "🔴 Encerrado"
        elif diff_horas <= 72: # Menos de 3 dias
            return "⚠️ URGENTE (72h)"
        else:
            return "✅ Aberto"

    df['Status'] = df.apply(definir_status, axis=1)

    # Sidebar com Filtros
    st.sidebar.header("Filtros de Busca")
    status_opcoes = df['Status'].unique().tolist()
    status_sel = st.sidebar.multiselect("Ver por Status:", status_opcoes, default=status_opcoes)
    
    # Busca por Órgão ou Título
    busca = st.sidebar.text_input("Buscar Órgão/Edital:")

    # Aplicando filtros
    df_filtrado = df[df['Status'].isin(status_sel)]
    if busca:
        df_filtrado = df_filtrado[
            df_filtrado['orgao'].str.contains(busca, case=False, na=False) | 
            df_filtrado['titulo'].str.contains(busca, case=False, na=False)
        ]

    # KPIs no topo
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Encontrado", len(df_filtrado))
    c2.metric("⚠️ Críticos (72h)", len(df[df['Status'] == "⚠️ URGENTE (72h)"]))
    c3.metric("✅ Abertos", len(df[df['Status'] == "✅ Aberto"]))

    # Alerta visual se houver urgência
    if len(df[df['Status'] == "⚠️ URGENTE (72h)"]) > 0:
        st.warning("⚠️ Atenção: Existem editais com prazo de encerramento muito próximo!")

    # Tabela Final formatada
    # Usamos o 'url_id' para criar o link clicável (opcional, mas profissional)
    df_filtrado['Link PNCP'] = "https://pncp.gov.br/app/editais" + df_filtrado['url_id']
    
    # Seleção de colunas para exibição limpa
    exibir = ['Status', 'Fim Propostas', 'orgao', 'titulo', 'uf', 'modalidade']
    
    st.dataframe(
        df_filtrado[exibir].sort_values(by='Status', ascending=False),
        use_container_width=True,
        hide_index=True
    )
    
    st.caption("Dica: Clique no cabeçalho das colunas para ordenar por data ou órgão.")

else:
    st.info("O banco de dados está vazio ou ainda sendo atualizado pelo robô.")

# Rodapé
st.divider()
st.markdown(f"**Data Specialist:** Igor Souza | **Última Atualização:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")
