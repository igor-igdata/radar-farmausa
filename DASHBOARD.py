import streamlit as st
import requests
import pandas as pd

# 1. Configuração da Página
st.set_page_config(page_title="Radar FarmaUSA - PNCP", page_icon="🌿", layout="wide")

# 2. Credenciais do Supabase
SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
SUPABASE_KEY = "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# 3. Função para extrair e cruzar os dados com proteção contra falhas de rede
@st.cache_data(ttl=300) # Mantém os dados em cache por 5 minutos
def carregar_dados():
    try:
        # Vai buscar os Editais (com limite de espera de 15 segundos)
        url_editais = f"{SUPABASE_URL}/rest/v1/editais_pncp?select=*"
        res_editais = requests.get(url_editais, headers=headers, timeout=15)
        res_editais.raise_for_status() # Verifica se ocorreu algum erro na resposta
        df_editais = pd.DataFrame(res_editais.json())
        
        # Vai buscar os Itens (com limite de espera de 15 segundos)
        url_itens = f"{SUPABASE_URL}/rest/v1/itens_pncp?select=*"
        res_itens = requests.get(url_itens, headers=headers, timeout=15)
        res_itens.raise_for_status()
        df_itens = pd.DataFrame(res_itens.json())
        
        if df_editais.empty or df_itens.empty:
            return pd.DataFrame()
            
        # Cruza as duas tabelas (equivalente ao Relacionamento)
        df_completo = pd.merge(df_itens, df_editais, left_on="edital_url_id", right_on="url_id", how="left")
        
        # Cria o link clicável
        df_completo['Link_Direto'] = "https://pncp.gov.br/app/editais" + df_completo['url_id'].str.replace('/compras', '')
        
        # Formata a data (adicionado errors='coerce' para maior segurança)
        df_completo['data_publicacao'] = pd.to_datetime(df_completo['data_publicacao'], errors='coerce').dt.strftime('%d/%m/%Y')
        
        return df_completo
        
    except requests.exceptions.Timeout:
        # Mensagem amigável caso a rede demore muito
        st.warning("⚠️ A rede demorou muito a responder. Tente atualizar a página.")
        return pd.DataFrame()
    except Exception as e:
        # Proteção geral contra outros erros
        st.error(f"⚠️ Erro ao ligar à base de dados: {e}")
        return pd.DataFrame()

df = carregar_dados()

# 4. Interface da Aplicação
st.title("🏛️ Radar de Licitações - Cannabis Medicinal")
st.markdown("Monitorização em tempo real de oportunidades no PNCP para a equipa comercial.")

if not df.empty:
    # --- Barra Lateral de Filtros ---
    st.sidebar.header("Filtros de Pesquisa")
    
    estados_unicos = df['uf'].dropna().unique().tolist()
    filtro_uf = st.sidebar.multiselect("Filtrar por Estado (UF):", estados_unicos, default=estados_unicos)
    
    modalidades_unicas = df['modalidade'].dropna().unique().tolist()
    filtro_mod = st.sidebar.multiselect("Filtrar por Modalidade:", modalidades_unicas, default=modalidades_unicas)
    
    # Aplica os filtros
    df_filtrado = df[(df['uf'].isin(filtro_uf)) & (df['modalidade'].isin(filtro_mod))]
    
    # --- Indicadores Chave (KPIs) ---
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total de Frascos/Itens Solicitados", f"{df_filtrado['quantidade'].sum():,.0f}".replace(',', '.'))
    with col2:
        st.metric("Oportunidades (Editais)", len(df_filtrado['url_id'].unique()))
    with col3:
        st.metric("Estados Mapeados", len(df_filtrado['uf'].unique()))
        
    st.divider()
    
    # --- Tabela de Oportunidades ---
    st.subheader("📋 Lista de Oportunidades Abertas")
    
    # Seleciona as colunas mais importantes para a equipa de vendas ler facilmente
    colunas_visiveis = ['data_publicacao', 'uf', 'orgao', 'modalidade', 'descricao', 'quantidade', 'valor_unitario', 'Link_Direto']
    df_exibicao = df_filtrado[colunas_visiveis].rename(columns={
        'data_publicacao': 'Data',
        'uf': 'UF',
        'orgao': 'Órgão Comprador',
        'modalidade': 'Modalidade',
        'descricao': 'Produto Solicitado',
        'quantidade': 'Qtd.',
        'valor_unitario': 'Preço Teto (R$)'
    })
    
    # Mostra a tabela com o link clicável
    st.dataframe(
        df_exibicao,
        column_config={
            "Link_Direto": st.column_config.LinkColumn("Acesso ao PNCP", display_text="Abrir Edital 🔗"),
            "Preço Teto (R$)": st.column_config.NumberColumn("Preço Teto (R$)", format="R$ %.2f")
        },
        hide_index=True,
        use_container_width=True
    )
else:
    st.info("A aguardar dados da base de dados...")