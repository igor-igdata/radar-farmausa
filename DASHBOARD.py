"""
DASHBOARD - Radar FarmaUSA (v3)
================================
Dashboard Streamlit para monitoramento de licitações de cannabis no PNCP.

Melhorias v3:
- Link direto na tabela (column_config.LinkColumn)
- KPI valor total usa valor_total_estimado do banco (correto)
- Campo objeto exibido no detalhe de cada edital
- Tabela de preços com UF + órgão + data (inteligência de mercado real)
- Coluna Valor Total na tabela principal
- Segundo gráfico: Valor por UF e Valor por Modalidade
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

CACHE_TTL = 120  # segundos

@st.cache_data(ttl=CACHE_TTL)
def carregar_editais():
    try:
        url = f"{SUPABASE_URL}/rest/v1/editais_pncp?select=*&order=data_publicacao.desc"
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return pd.DataFrame(r.json())
    except Exception as e:
        st.error(f"Erro ao carregar editais: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL)
def carregar_itens():
    try:
        url = f"{SUPABASE_URL}/rest/v1/itens_pncp?select=*"
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        if df.empty:
            return pd.DataFrame()

        if "quantidade" in df.columns:
            df = df[df["quantidade"].fillna(0) <= QTD_MAX_RAZOAVEL]

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
    if df_editais.empty:
        return pd.DataFrame()

    hoje = datetime.now()  # naive, sem timezone (compatível com datas convertidas)

    def parse_dt(col):
        """Converte coluna de data do Supabase para datetime naive, tratando mixed timezones."""
        from zoneinfo import ZoneInfo
        tz_br = ZoneInfo("America/Sao_Paulo")
        def _parse_single(val):
            if not val or pd.isna(val):
                return pd.NaT
            try:
                dt = pd.to_datetime(val)
                if dt.tzinfo is not None:
                    dt = dt.tz_convert(tz_br).replace(tzinfo=None)
                return dt
            except Exception:
                return pd.NaT
        return pd.Series([_parse_single(v) for v in col], index=col.index)

    df_editais["dt_fim"] = parse_dt(df_editais["data_fim"])
    df_editais["dt_inicio"] = parse_dt(df_editais["data_inicio"])
    df_editais["dt_pub"] = parse_dt(df_editais["data_publicacao"])

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

    def montar_link(url_id):
        if not url_id:
            return ""
        partes = str(url_id).strip("/").split("/")
        if len(partes) >= 4 and partes[0] == "compras":
            return f"https://pncp.gov.br/app/editais/{partes[1]}/{partes[2]}/{partes[3]}"
        return f"https://pncp.gov.br/app/editais{url_id}"

    df_editais["link_pncp"] = df_editais["url_id"].apply(montar_link)

    df_editais["Abertura"] = df_editais["dt_inicio"].dt.strftime("%d/%m/%Y %H:%M").fillna("—")
    df_editais["Encerramento"] = df_editais["dt_fim"].dt.strftime("%d/%m/%Y %H:%M").fillna("—")
    df_editais["Publicação"] = df_editais["dt_pub"].dt.strftime("%d/%m/%Y").fillna("—")

    # JOIN com itens
    if not df_itens.empty and "edital_url_id" in df_itens.columns:
        agg = df_itens.groupby("edital_url_id").agg(
            qtd_total=("quantidade", "sum"),
            n_itens=("id", "count"),
            preco_unit_max=("valor_unitario", "max"),
            preco_unit_min=("valor_unitario", "min"),
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

    def resumir_produto(row):
        desc = str(row.get("_descricao_max_qtd", "") or "")
        if desc and len(desc) > 5:
            return desc[:120]
        return str(row.get("objeto", row.get("titulo", "—")) or "—")[:120]

    df["produto"] = df.apply(resumir_produto, axis=1)

    def formatar_preco(val):
        if pd.isna(val) or val is None:
            return "—"
        return f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    df["preco_unit_fmt"] = df["preco_unit_max"].apply(formatar_preco)

    df["qtd_fmt"] = df["qtd_total"].apply(
        lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) and x > 0 else "—"
    )

    # Valor total formatado — usa valor_total_estimado do banco (correto após migração)
    def formatar_valor_total(val):
        try:
            f = float(val)
            if f <= 0:
                return "—"
            return f"R$ {f:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return "—"

    df["valor_total_fmt"] = df["valor_total_estimado"].apply(formatar_valor_total)

    return df


# ─── Interface ────────────────────────────────────────────────────────────────

st.title("🌿 Radar de Licitações — Cannabis Medicinal")
st.caption("Monitoramento em tempo real de oportunidades no PNCP para a equipe comercial.")

with st.spinner("Carregando dados..."):
    df_editais = carregar_editais()
    df_itens = carregar_itens()
    if "ts_carregamento" not in st.session_state:
        st.session_state["ts_carregamento"] = datetime.now()

if df_editais.empty:
    st.info("O banco de dados está vazio ou ainda sendo atualizado.")
    st.stop()

df = processar_dados(df_editais, df_itens)

# ─── Sidebar — Filtros ────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔍 Filtros")

    status_opts = ["✅ Aberto", "⚠️ Urgente", "⚪ Sem data", "🔴 Encerrado"]
    status_default = ["✅ Aberto", "⚠️ Urgente", "⚪ Sem data"]
    status_sel = st.multiselect("Status", status_opts, default=status_default)

    ufs = sorted(df["uf"].dropna().unique().tolist())
    uf_sel = st.multiselect("Estado (UF)", ufs, default=[])

    mods = sorted(df["modalidade"].dropna().unique().tolist())
    mod_sel = st.multiselect("Modalidade", mods, default=[])

    busca = st.text_input("Buscar órgão ou produto:", placeholder="ex: Secretaria, Pregão...")

    st.divider()

    # Contador total vs filtrado — calculado após aplicar filtros (atualizado via session_state)
    total_banco = len(df)
    if "n_filtrado" not in st.session_state:
        st.session_state["n_filtrado"] = total_banco
    n_filt = st.session_state.get("n_filtrado", total_banco)
    if n_filt < total_banco:
        st.markdown(f"**🔎 Exibindo {n_filt} de {total_banco} editais**")
    else:
        st.markdown(f"**📊 {total_banco} editais no banco**")

    # Indicador de cache
    ts_carregamento = st.session_state.get("ts_carregamento", datetime.now())
    segundos_passados = int((datetime.now() - ts_carregamento).total_seconds())
    segundos_restantes = max(0, CACHE_TTL - segundos_passados)
    st.caption(
        f"Atualizado: {ts_carregamento.strftime('%d/%m/%Y %H:%M')}  \n"
        f"Cache expira em ~{segundos_restantes}s"
    )
    if st.button("🔄 Recarregar dados"):
        st.cache_data.clear()
        st.session_state["ts_carregamento"] = datetime.now()
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
        df_f["objeto"].fillna("").str.contains(busca, case=False, na=False) |
        df_f["modalidade"].str.contains(busca, case=False, na=False)
    )
    df_f = df_f[mask]

# Atualiza contador da sidebar
st.session_state["n_filtrado"] = len(df_f)

# ─── KPIs ─────────────────────────────────────────────────────────────────────
df_ativos = df_f[df_f["status"].isin(["✅ Aberto", "⚠️ Urgente"])]

total_frascos = df_ativos["qtd_total"].fillna(0).sum()
total_editais = len(df_ativos)
total_estados = df_ativos["uf"].nunique()
urgentes = len(df_f[df_f["status"] == "⚠️ Urgente"])

# Valor correto: usa valor_total_estimado do banco
valor_total = df_ativos["valor_total_estimado"].fillna(0).astype(float).sum()

def fmt_moeda(v):
    if v <= 0:
        return "—"
    return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

# Delta — snapshot guardado na session_state, renovado a cada 7 dias
_snap_key, _snap_ts_key = "kpi_snapshot", "kpi_snapshot_ts"
_agora = datetime.now()
_snap = st.session_state.get(_snap_key)
_snap_ts = st.session_state.get(_snap_ts_key)

if _snap is None or (_snap_ts and (_agora - _snap_ts).days >= 7):
    st.session_state[_snap_key] = {
        "editais": total_editais, "frascos": int(total_frascos),
        "valor": valor_total, "urgentes": urgentes,
    }
    st.session_state[_snap_ts_key] = _agora
    _snap = st.session_state[_snap_key]

_d_editais = total_editais  - _snap.get("editais",  total_editais)
_d_frascos = int(total_frascos) - _snap.get("frascos", int(total_frascos))
_d_valor   = valor_total    - _snap.get("valor",   valor_total)
_d_urg     = urgentes       - _snap.get("urgentes", urgentes)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("📦 Frascos Solicitados",
          f"{int(total_frascos):,}".replace(",", "."),
          delta=f"{_d_frascos:+,}".replace(",", ".") if _d_frascos != 0 else None)
c2.metric("💰 Valor Estimado",
          fmt_moeda(valor_total),
          delta=fmt_moeda(abs(_d_valor)) if _d_valor != 0 else None)
c3.metric("📋 Editais Ativos",
          total_editais,
          delta=f"{_d_editais:+d}" if _d_editais != 0 else None)
c4.metric("🗺️ Estados", total_estados)
c5.metric("⚠️ Urgentes (72h)",
          urgentes,
          delta=f"{_d_urg:+d}" if _d_urg != 0 else None,
          delta_color="inverse")

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
            objeto_txt = str(row.get("objeto", "") or "").strip()
            objeto_html = f'<br><span style="color:#aaa;font-size:12px">📝 {objeto_txt[:180]}</span>' if objeto_txt else ""
            st.markdown(f"""
<div class="urgente-card">
  <div class="urgente-titulo">🔴 {row.get('prazo', '')} &nbsp;|&nbsp; {row.get('uf', '')} &nbsp;|&nbsp; {row.get('orgao', '')}</div>
  <div class="urgente-detalhe">
    {row.get('produto', '—')[:100]}{objeto_html}<br>
    📦 {row.get('qtd_fmt', '—')} frascos &nbsp;|&nbsp; 💲 {row.get('preco_unit_fmt', '—')} unit &nbsp;|&nbsp; 💰 {row.get('valor_total_fmt', '—')} &nbsp;|&nbsp;
    <a href="{link}" target="_blank">Abrir Edital →</a>
  </div>
</div>
""", unsafe_allow_html=True)
        st.divider()

    # Tabela principal
    st.markdown("### 📋 Lista de Oportunidades")

    df_exib = df_f.copy()
    # Remove colunas string formatadas que conflitam com as colunas datetime/numéricas
    df_exib = df_exib.drop(columns=["Publicação", "Abertura", "Encerramento"], errors="ignore")
    ordem_status = {"⚠️ Urgente": 0, "✅ Aberto": 1, "⚪ Sem data": 2, "🔴 Encerrado": 3}
    df_exib["_ordem"] = df_exib["status"].map(ordem_status).fillna(9)
    df_exib = df_exib.sort_values(["_ordem", "dt_fim"], na_position="last")

    colunas_exib = {
        "dt_pub": "Publicação",
        "uf": "UF",
        "orgao": "Órgão",
        "modalidade": "Modalidade",
        "produto": "Produto",
        "qtd_total": "Qtd.",
        "preco_unit_max": "Preço Unit.",
        "valor_total_estimado": "Valor Total",
        "prazo": "Prazo",
        "status": "Status",
        "link_pncp": "Edital",
    }

    df_tabela = df_exib.rename(columns=colunas_exib)[list(colunas_exib.values())]

    # Link direto na tabela — sem expander separado
    st.dataframe(
        df_tabela,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Publicação": st.column_config.DateColumn(
                "Publicação",
                format="DD/MM/YYYY",
                width="small",
            ),
            "Qtd.": st.column_config.NumberColumn(
                "Qtd.",
                format="%d",
                width="small",
            ),
            "Preço Unit.": st.column_config.NumberColumn(
                "Preço Unit.",
                format="R$ %.2f",
                width="medium",
            ),
            "Valor Total": st.column_config.NumberColumn(
                "Valor Total",
                format="R$ %.2f",
                width="medium",
            ),
            "Edital": st.column_config.LinkColumn(
                "Edital",
                display_text="Abrir ↗",
                width="small",
            ),
            "Prazo": st.column_config.TextColumn("Prazo", width="medium"),
            "Status": st.column_config.TextColumn("Status", width="small"),
            "UF": st.column_config.TextColumn("UF", width="small"),
        }
    )

    # Expander com objeto completo
    with st.expander("📝 Objeto completo dos editais"):
        for _, row in df_exib.iterrows():
            objeto = str(row.get("objeto", "") or "").strip()
            titulo = str(row.get("titulo", "") or "").strip()
            link = row.get("link_pncp", "")
            uf = row.get("uf", "")
            orgao = str(row.get("orgao", "") or "")[:60]
            texto = f"**{uf} | {orgao}** — {titulo}  \n📝 {objeto if objeto else '*(sem objeto cadastrado)*'}"
            if link:
                texto += f"  \n[Abrir no PNCP]({link})"
            st.markdown(texto)
            st.divider()

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
                .sum().fillna(0).sort_values(ascending=False).reset_index()
            )
            por_uf.columns = ["UF", "Frascos"]
            por_uf["Frascos"] = por_uf["Frascos"].astype(int)
            st.bar_chart(por_uf.set_index("UF"), color="#00cc88")

        with col2:
            st.markdown("#### Valor Estimado por Estado (R$)")
            por_uf_val = (
                df_anal.groupby("uf")["valor_total_estimado"]
                .sum().fillna(0).sort_values(ascending=False).reset_index()
            )
            por_uf_val.columns = ["UF", "Valor (R$)"]
            st.bar_chart(por_uf_val.set_index("UF"), color="#ffaa00")

        col3, col4 = st.columns(2)

        with col3:
            st.markdown("#### Editais por Modalidade")
            por_mod = (
                df_anal.groupby("modalidade").size()
                .reset_index(name="Editais").sort_values("Editais", ascending=False)
            )
            st.bar_chart(por_mod.set_index("modalidade"), color="#4488ff")

        with col4:
            st.markdown("#### Valor por Modalidade (R$)")
            por_mod_val = (
                df_anal.groupby("modalidade")["valor_total_estimado"]
                .sum().fillna(0).sort_values(ascending=False).reset_index()
            )
            por_mod_val.columns = ["Modalidade", "Valor (R$)"]
            st.bar_chart(por_mod_val.set_index("Modalidade"), color="#cc44ff")

        st.divider()

        # Tabela de preços com contexto de mercado
        st.markdown("#### 💲 Referência de Preços — Inteligência de Mercado")
        st.caption("Preços unitários praticados por órgão público — base para precificação e proposta comercial.")

        if not df_itens.empty:
            df_preco = df_itens[df_itens["valor_unitario"].notna()].copy()
            df_preco = df_preco[df_preco["valor_unitario"] > 0]

            if not df_preco.empty:
                # JOIN com editais para trazer UF, órgão e data
                df_meta = df[["url_id", "uf", "orgao", "dt_pub", "link_pncp"]].rename(
                    columns={"url_id": "edital_url_id"}
                )
                df_preco = df_preco.merge(df_meta, on="edital_url_id", how="left")

                df_preco = df_preco.rename(columns={
                    "descricao": "Produto", "quantidade": "Qtd", "valor_unitario": "Preço Unit. (R$)",
                    "uf": "UF", "orgao": "Órgão", "dt_pub": "Publicação", "link_pncp": "Edital"
                })

                st.dataframe(
                    df_preco[["Produto", "Qtd", "Preço Unit. (R$)", "UF", "Órgão", "Publicação", "Edital"]]
                    .sort_values(["Produto", "Preço Unit. (R$)"]),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Qtd": st.column_config.NumberColumn("Qtd", format="%d", width="small"),
                        "Preço Unit. (R$)": st.column_config.NumberColumn("Preço Unit. (R$)", format="R$ %.2f", width="medium"),
                        "Publicação": st.column_config.DateColumn("Publicação", format="DD/MM/YYYY", width="small"),
                        "Edital": st.column_config.LinkColumn("Edital", display_text="Ver ↗", width="small"),
                        "UF": st.column_config.TextColumn("UF", width="small"),
                    }
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
