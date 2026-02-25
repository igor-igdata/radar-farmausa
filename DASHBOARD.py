"""
DASHBOARD - Radar FarmaUSA (v2)
================================
Dashboard Streamlit para monitoramento de licitações de cannabis no PNCP.

Melhorias v2:
- KPIs de negócio: frascos, valor total, estados, editais ativos
- JOIN com itens_pncp: mostra produto real, qtd e preço unitário
- Filtros por UF, modalidade, prazo e busca textual
- Bloco de alertas urgentes (< 72h) destacado no topo
- Link clicável correto para o PNCP
- Aba de análise com gráficos por UF e modalidade
- Tratamento de dados inconsistentes (qtd absurda, itens não-cannabis)
"""

import streamlit as st
import requests
import pandas as pd
from datetime import datetime

# ─── Configuração ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Radar FarmaUSA - Licitações Cannabis",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
SUPABASE_KEY = "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# Quantidade máxima razoável de frascos num único item (filtro anti-lixo)
QTD_MAX_RAZOAVEL = 100_000

# ─── CSS customizado ──────────────────────────────────────────────────────────
st.markdown("""
<style>
    .urgente-card {
        background: #3d1a1a;
        border-left: 4px solid #ff4444;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
    .urgente-titulo { color: #ff6b6b; font-weight: bold; font-size: 14px; }
    .urgente-detalhe { color: #cccccc; font-size: 13px; margin-top: 4px; }
    .kpi-label { font-size: 13px; color: #aaaaaa; }
    .kpi-valor { font-size: 28px; font-weight: bold; color: #ffffff; }
</style>
""", unsafe_allow_html=True)


# ─── Carregamento de dados ────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def carregar_editais():
    """Busca todos os editais do Supabase."""
    try:
        url = f"{SUPABASE_URL}/rest/v1/editais_pncp?select=*&order=data_publicacao.desc"
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return pd.DataFrame(r.json())
    except Exception as e:
        st.error(f"Erro ao carregar editais: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=120)
def carregar_itens():
    """Busca todos os itens do Supabase."""
    try:
        url = f"{SUPABASE_URL}/rest/v1/itens_pncp?select=*"
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        if df.empty:
            return pd.DataFrame()

        # Remove itens com quantidade absurda (erro de dado da API)
        if "quantidade" in df.columns:
            df = df[df["quantidade"].fillna(0) <= QTD_MAX_RAZOAVEL]

        # Remove falsos positivos óbvios (ex: poste com "CBD" na sigla)
        keywords_cannabis = ["canabidiol", "cannabis", "cbd", "cannabidiol", "thc", "cânhamo", "extrato medicinal"]
        if "descricao" in df.columns:
            mask = df["descricao"].str.lower().apply(
                lambda x: any(k in str(x) for k in keywords_cannabis)
            )
            df = df[mask]

        return df
    except Exception as e:
        st.error(f"Erro ao carregar itens: {e}")
        return pd.DataFrame()


def processar_dados(df_editais, df_itens):
    """
    Faz o JOIN entre editais e itens, calcula status e prazo.
    Retorna o dataframe principal enriquecido.
    """
    if df_editais.empty:
        return pd.DataFrame()

    hoje = datetime.now()

    # Datas
    df_editais["dt_fim"] = pd.to_datetime(df_editais["data_fim"], errors="coerce")
    df_editais["dt_inicio"] = pd.to_datetime(df_editais["data_inicio"], errors="coerce")
    df_editais["dt_pub"] = pd.to_datetime(df_editais["data_publicacao"], errors="coerce")

    # Status e prazo
    def calcular_status(row):
        if pd.isna(row["dt_fim"]):
            return "⚪ Sem data"
        diff_h = (row["dt_fim"] - hoje).total_seconds() / 3600
        if diff_h < 0:
            return "🔴 Encerrado"
        elif diff_h <= 72:
            return "⚠️ Urgente"
        else:
            return "✅ Aberto"

    def calcular_prazo_texto(row):
        if pd.isna(row["dt_fim"]):
            return "—"
        diff_h = (row["dt_fim"] - hoje).total_seconds() / 3600
        if diff_h < 0:
            dias = int(abs(diff_h) // 24)
            return f"Encerrou há {dias}d"
        elif diff_h < 24:
            return f"⚠️ {int(diff_h)}h restantes"
        else:
            dias = int(diff_h // 24)
            return f"{dias} dias"

    df_editais["status"] = df_editais.apply(calcular_status, axis=1)
    df_editais["prazo"] = df_editais.apply(calcular_prazo_texto, axis=1)

    # Link correto: remove "/compras/" do url_id → /compras/CNPJ/ANO/SEQ → CNPJ/ANO/SEQ
    def montar_link(url_id):
        if not url_id:
            return ""
        partes = str(url_id).strip("/").split("/")
        # formato: compras/{cnpj}/{ano}/{seq}
        if len(partes) >= 4 and partes[0] == "compras":
            return f"https://pncp.gov.br/app/editais/{partes[1]}/{partes[2]}/{partes[3]}"
        return f"https://pncp.gov.br/app/editais{url_id}"

    df_editais["link_pncp"] = df_editais["url_id"].apply(montar_link)

    # Datas formatadas para exibição
    df_editais["Abertura"] = df_editais["dt_inicio"].dt.strftime("%d/%m/%Y %H:%M").fillna("—")
    df_editais["Encerramento"] = df_editais["dt_fim"].dt.strftime("%d/%m/%Y %H:%M").fillna("—")
    df_editais["Publicação"] = df_editais["dt_pub"].dt.strftime("%d/%m/%Y").fillna("—")

    # JOIN com itens — agrega por edital
    if not df_itens.empty and "edital_url_id" in df_itens.columns:
        agg = df_itens.groupby("edital_url_id").agg(
            qtd_total=("quantidade", "sum"),
            n_itens=("id", "count"),
            preco_unit_max=("valor_unitario", "max"),
            preco_unit_min=("valor_unitario", "min"),
            # Produto: pega a descrição do item de maior quantidade
            _descricao_max_qtd=("descricao", lambda x: x.iloc[
                df_itens.loc[x.index, "quantidade"].fillna(0).argmax()
            ] if len(x) > 0 else ""),
        ).reset_index()
        agg.rename(columns={"edital_url_id": "url_id"}, inplace=True)

        df = df_editais.merge(agg, on="url_id", how="left")
    else:
        df = df_editais.copy()
        df["qtd_total"] = 0
        df["n_itens"] = 0
        df["preco_unit_max"] = None
        df["preco_unit_min"] = None
        df["_descricao_max_qtd"] = ""

    # Produto resumido: usa descrição do item ou objeto do edital como fallback
    def resumir_produto(row):
        desc = str(row.get("_descricao_max_qtd", "") or "")
        if desc and len(desc) > 5:
            return desc[:120]
        return str(row.get("objeto", row.get("titulo", "—")) or "—")[:120]

    df["produto"] = df.apply(resumir_produto, axis=1)

    # Preço unitário formatado
    def formatar_preco(val):
        if pd.isna(val) or val is None:
            return "—"
        return f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    df["preco_unit_fmt"] = df["preco_unit_max"].apply(formatar_preco)

    # Qtd formatada
    df["qtd_fmt"] = df["qtd_total"].apply(
        lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) and x > 0 else "—"
    )

    return df


# ─── Interface ────────────────────────────────────────────────────────────────

st.title("🌿 Radar de Licitações — Cannabis Medicinal")
st.caption("Monitoramento em tempo real de oportunidades no PNCP para a equipe comercial.")

# Carrega dados
with st.spinner("Carregando dados..."):
    df_editais = carregar_editais()
    df_itens = carregar_itens()

if df_editais.empty:
    st.info("O banco de dados está vazio ou ainda sendo atualizado.")
    st.stop()

df = processar_dados(df_editais, df_itens)

# ─── Sidebar — Filtros ────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔍 Filtros")

    # Status
    status_opts = ["✅ Aberto", "⚠️ Urgente", "⚪ Sem data", "🔴 Encerrado"]
    status_default = ["✅ Aberto", "⚠️ Urgente", "⚪ Sem data"]
    status_sel = st.multiselect("Status", status_opts, default=status_default)

    # UF
    ufs = sorted(df["uf"].dropna().unique().tolist())
    uf_sel = st.multiselect("Estado (UF)", ufs, default=[])

    # Modalidade
    mods = sorted(df["modalidade"].dropna().unique().tolist())
    mod_sel = st.multiselect("Modalidade", mods, default=[])

    # Busca textual
    busca = st.text_input("Buscar órgão ou produto:", placeholder="ex: Secretaria, Pregão...")

    st.divider()
    st.caption(f"Atualizado: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    if st.button("🔄 Recarregar dados"):
        st.cache_data.clear()
        st.rerun()

# ─── Aplicar filtros ──────────────────────────────────────────────────────────
df_f = df.copy()

if status_sel:
    df_f = df_f[df_f["status"].isin(status_sel)]
if uf_sel:
    df_f = df_f[df_f["uf"].isin(uf_sel)]
if mod_sel:
    df_f = df_f[df_f["modalidade"].isin(mod_sel)]
if busca:
    mask = (
        df_f["orgao"].str.contains(busca, case=False, na=False) |
        df_f["produto"].str.contains(busca, case=False, na=False) |
        df_f["modalidade"].str.contains(busca, case=False, na=False)
    )
    df_f = df_f[mask]

# ─── KPIs ─────────────────────────────────────────────────────────────────────
df_ativos = df_f[df_f["status"].isin(["✅ Aberto", "⚠️ Urgente"])]

total_frascos = df_ativos["qtd_total"].fillna(0).sum()
total_editais = len(df_ativos)
total_estados = df_ativos["uf"].nunique()
urgentes = len(df_f[df_f["status"] == "⚠️ Urgente"])

# Valor total: qtd × preço_unit quando ambos disponíveis
df_ativos_val = df_ativos.copy()
df_ativos_val["valor_calc"] = df_ativos_val["qtd_total"].fillna(0) * df_ativos_val["preco_unit_max"].fillna(0)
valor_total = df_ativos_val["valor_calc"].sum()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("📦 Frascos Solicitados", f"{int(total_frascos):,}".replace(",", "."))
c2.metric("💰 Valor Estimado", f"R$ {valor_total:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".") if valor_total > 0 else "—")
c3.metric("📋 Editais Ativos", total_editais)
c4.metric("🗺️ Estados", total_estados)
c5.metric("⚠️ Urgentes (72h)", urgentes)

st.divider()

# ─── Abas ─────────────────────────────────────────────────────────────────────
aba1, aba2 = st.tabs(["📋 Oportunidades", "📊 Análise"])

# ══════════════════════════════════════════════════════════════════════════════
with aba1:

    # Bloco de alertas urgentes
    df_urgentes = df_f[df_f["status"] == "⚠️ Urgente"].sort_values("dt_fim")
    if not df_urgentes.empty:
        st.markdown(f"### ⚠️ Atenção — {len(df_urgentes)} edital(is) encerram em menos de 72h")
        for _, row in df_urgentes.iterrows():
            link = row.get("link_pncp", "")
            st.markdown(f"""
<div class="urgente-card">
  <div class="urgente-titulo">🔴 {row.get('prazo', '')} &nbsp;|&nbsp; {row.get('uf', '')} &nbsp;|&nbsp; {row.get('orgao', '')}</div>
  <div class="urgente-detalhe">
    {row.get('produto', '—')[:100]}<br>
    📦 {row.get('qtd_fmt', '—')} frascos &nbsp;|&nbsp; 💲 {row.get('preco_unit_fmt', '—')} unit &nbsp;|&nbsp;
    <a href="{link}" target="_blank">Abrir Edital →</a>
  </div>
</div>
""", unsafe_allow_html=True)
        st.divider()

    # Tabela principal
    st.markdown("### 📋 Lista de Oportunidades")

    # Monta dataframe de exibição
    df_exib = df_f.copy()

    # Coluna de link como HTML clicável
    df_exib["Edital"] = df_exib.apply(
        lambda r: f'<a href="{r["link_pncp"]}" target="_blank">Abrir ↗</a>' if r.get("link_pncp") else "—",
        axis=1
    )

    # Ordenação padrão: urgentes primeiro, depois por data de encerramento
    ordem_status = {"⚠️ Urgente": 0, "✅ Aberto": 1, "⚪ Sem data": 2, "🔴 Encerrado": 3}
    df_exib["_ordem"] = df_exib["status"].map(ordem_status).fillna(9)
    df_exib = df_exib.sort_values(["_ordem", "dt_fim"], na_position="last")

    colunas_exib = {
        "Publicação": "Publicação",
        "uf": "UF",
        "orgao": "Órgão",
        "modalidade": "Modalidade",
        "produto": "Produto Solicitado",
        "qtd_fmt": "Qtd.",
        "preco_unit_fmt": "Preço Unit. (R$)",
        "prazo": "Prazo",
        "status": "Status",
    }

    df_tabela = df_exib.rename(columns=colunas_exib)[list(colunas_exib.values()) + ["link_pncp"]]

    # Exibe com st.dataframe + coluna de link configurada
    st.dataframe(
        df_tabela.drop(columns=["link_pncp"]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Qtd.": st.column_config.TextColumn("Qtd.", width="small"),
            "Preço Unit. (R$)": st.column_config.TextColumn("Preço Unit.", width="medium"),
            "Prazo": st.column_config.TextColumn("Prazo", width="medium"),
            "Status": st.column_config.TextColumn("Status", width="small"),
            "UF": st.column_config.TextColumn("UF", width="small"),
        }
    )

    # Links separados abaixo da tabela (Streamlit não suporta HTML em dataframe)
    with st.expander("🔗 Links dos Editais"):
        for _, row in df_exib.iterrows():
            if row.get("link_pncp"):
                st.markdown(
                    f"**{row.get('uf', '')} | {row.get('orgao', '')[:60]}** — "
                    f"[Abrir no PNCP]({row['link_pncp']})"
                )

    st.caption(f"Exibindo {len(df_f)} editais. Clique no cabeçalho das colunas para ordenar.")


# ══════════════════════════════════════════════════════════════════════════════
with aba2:
    st.markdown("### 📊 Análise de Oportunidades")

    df_anal = df_f[df_f["status"].isin(["✅ Aberto", "⚠️ Urgente", "⚪ Sem data"])]

    if df_anal.empty:
        st.info("Sem dados ativos para análise com os filtros atuais.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Frascos por Estado (UF)")
            por_uf = (
                df_anal.groupby("uf")["qtd_total"]
                .sum()
                .fillna(0)
                .sort_values(ascending=False)
                .reset_index()
            )
            por_uf.columns = ["UF", "Frascos"]
            por_uf["Frascos"] = por_uf["Frascos"].astype(int)
            st.bar_chart(por_uf.set_index("UF"), color="#00cc88")

        with col2:
            st.markdown("#### Editais por Modalidade")
            por_mod = (
                df_anal.groupby("modalidade")
                .size()
                .reset_index(name="Editais")
                .sort_values("Editais", ascending=False)
            )
            st.bar_chart(por_mod.set_index("modalidade"), color="#4488ff")

        st.divider()

        # Tabela de preços por produto (inteligência de mercado)
        st.markdown("#### 💲 Referência de Preços — Itens com Valor Informado")

        if not df_itens.empty:
            df_preco = df_itens[df_itens["valor_unitario"].notna()].copy()
            df_preco = df_preco[df_preco["valor_unitario"] > 0]

            if not df_preco.empty:
                df_preco_exib = df_preco[["descricao", "quantidade", "valor_unitario"]].copy()
                df_preco_exib.columns = ["Produto", "Qtd", "Preço Unit. (R$)"]
                df_preco_exib["Preço Unit. (R$)"] = df_preco_exib["Preço Unit. (R$)"].apply(
                    lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                )
                df_preco_exib["Qtd"] = df_preco_exib["Qtd"].apply(
                    lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "—"
                )
                st.dataframe(
                    df_preco_exib.sort_values("Produto"),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Sem itens com preço informado.")
        else:
            st.info("Tabela de itens não disponível.")

# ─── Rodapé ───────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    f"**Data Specialist:** Igor Souza &nbsp;|&nbsp; "
    f"**igdata.com.br** &nbsp;|&nbsp; "
    f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
)
