import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Config ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Adventure Works — Sales Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Palette ──────────────────────────────────────────────────────
C = {
    "primary":   "#586643",
    "secondary": "#7A8F5F",
    "accent":    "#3D4A2E",
    "light":     "#E8EDE3",
    "lighter":   "#F4F6F1",
    "white":     "#FFFFFF",
    "grey":      "#6B7280",
    "dark":      "#1F2937",
    "border":    "#D1D9C8",
    "orange":    "#D47C2F",
}

# ── CSS Responsive ───────────────────────────────────────────────
st.markdown(f"""
<style>
    /* Reset & base */
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}

    .stApp {{ background-color: {C['lighter']}; }}

    /* Hide streamlit default elements */
    #MainMenu, footer, header {{ visibility: hidden; }}
    .block-container {{
        padding: 0 !important;
        max-width: 100% !important;
    }}

    /* Header */
    .dash-header {{
        background: linear-gradient(135deg, {C['accent']} 0%, {C['primary']} 100%);
        padding: 20px 32px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 12px;
        margin-bottom: 0;
    }}
    .dash-header-title {{
        color: white;
        font-size: clamp(16px, 3vw, 24px);
        font-weight: 700;
        letter-spacing: -0.3px;
    }}
    .dash-header-sub {{
        color: {C['light']};
        font-size: clamp(11px, 1.5vw, 13px);
        opacity: 0.9;
    }}

    /* Nav pills */
    .nav-container {{
        background: {C['white']};
        padding: 16px 32px;
        border-bottom: 1px solid {C['border']};
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        align-items: center;
        justify-content: space-between;
    }}
    .nav-pills {{
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
    }}

    /* KPI Cards */
    .kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 16px;
        padding: 24px 32px 0;
    }}
    .kpi-card {{
        background: {C['white']};
        border-radius: 12px;
        padding: 20px 16px;
        border: 1px solid {C['border']};
        border-left: 4px solid {C['primary']};
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        transition: transform 0.2s, box-shadow 0.2s;
    }}
    .kpi-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(88,102,67,0.15);
    }}
    .kpi-icon {{
        font-size: 20px;
        margin-bottom: 8px;
    }}
    .kpi-value {{
        font-size: clamp(20px, 3vw, 28px);
        font-weight: 700;
        color: {C['primary']};
        line-height: 1.1;
    }}
    .kpi-label {{
        font-size: 12px;
        color: {C['grey']};
        margin-top: 4px;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    .kpi-delta {{
        font-size: 11px;
        margin-top: 6px;
        padding: 2px 8px;
        border-radius: 20px;
        display: inline-block;
    }}
    .kpi-delta-pos {{
        background: #DCFCE7;
        color: #15803D;
    }}
    .kpi-delta-neg {{
        background: #FEE2E2;
        color: #DC2626;
    }}

    /* Section title */
    .section-title {{
        font-size: 15px;
        font-weight: 600;
        color: {C['dark']};
        padding: 24px 32px 0;
        display: flex;
        align-items: center;
        gap: 8px;
    }}
    .section-title::after {{
        content: '';
        flex: 1;
        height: 1px;
        background: {C['border']};
        margin-left: 12px;
    }}

    /* Charts container */
    .charts-container {{
        padding: 16px 32px;
    }}

    /* Sidebar */
    .css-1d391kg, [data-testid="stSidebar"] {{
        background: {C['white']};
        border-right: 1px solid {C['border']};
    }}

    /* Responsive adjustments */
    @media (max-width: 768px) {{
        .dash-header {{ padding: 16px 16px; }}
        .kpi-grid {{ padding: 16px 16px 0; gap: 12px; }}
        .charts-container {{ padding: 12px 16px; }}
        .section-title {{ padding: 16px 16px 0; }}
        .nav-container {{ padding: 12px 16px; }}
    }}

    @media (max-width: 480px) {{
        .kpi-grid {{
            grid-template-columns: repeat(2, 1fr);
        }}
    }}

    /* Dataframe styling */
    .stDataFrame {{
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid {C['border']};
    }}

    /* Plotly charts */
    .js-plotly-plot {{
        border-radius: 12px;
    }}
</style>
""", unsafe_allow_html=True)

# ── Chargement données ───────────────────────────────────────────
BASE_URL = "https://raw.githubusercontent.com/AstreetM/adventureworks-dwh/main/data/csv/"

@st.cache_data(show_spinner="Chargement des données...")
def load_data():
    fs  = pd.read_csv(BASE_URL + "FactSales.csv")
    fh  = pd.read_csv(BASE_URL + "FactSalesHeader.csv")
    dp  = pd.read_csv(BASE_URL + "DimProduct.csv")
    dc  = pd.read_csv(BASE_URL + "DimCustomer.csv")
    dt  = pd.read_csv(BASE_URL + "DimTerritory.csv")
    dsh = pd.read_csv(BASE_URL + "DimShipMethod.csv")
    dd  = pd.read_csv(BASE_URL + "DimDate.csv")
    return fs, fh, dp, dc, dt, dsh, dd

fact_sales, fact_header, dim_product, dim_customer, dim_territory, dim_ship, dim_date = load_data()

# ── Jointures ────────────────────────────────────────────────────
fs = fact_sales.merge(dim_date[["date_key","year","month","month_name"]],
                       left_on="order_date_key", right_on="date_key", how="left")
fs = fs.merge(dim_product[["product_key","product_name","category","standard_cost"]],
               on="product_key", how="left")
fs = fs.merge(dim_customer[["customer_key","full_name","customer_type"]],
               on="customer_key", how="left")
fs = fs.merge(dim_territory[["territory_key","territory_name","continent"]],
               on="territory_key", how="left")

fh = fact_header.merge(dim_territory[["territory_key","territory_name"]],
                        on="territory_key", how="left")
fh = fh.merge(dim_ship[["shipmethod_key","ship_method_name"]],
               on="shipmethod_key", how="left")

mois_fr = {
    1:"Janvier", 2:"Février", 3:"Mars", 4:"Avril",
    5:"Mai", 6:"Juin", 7:"Juillet", 8:"Août",
    9:"Septembre", 10:"Octobre", 11:"Novembre", 12:"Décembre"
}
fs["mois_label"] = fs["month"].map(mois_fr)

# ── Layout helper ────────────────────────────────────────────────
def chart_layout(fig, title=""):
    fig.update_layout(
        title=dict(text=title, font=dict(size=14, color=C["dark"]), x=0),
        plot_bgcolor=C["white"],
        paper_bgcolor=C["white"],
        font=dict(color=C["grey"], size=12),
        margin=dict(t=40, b=40, l=10, r=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="left", x=0),
        hoverlabel=dict(bgcolor=C["white"], bordercolor=C["border"],
                        font_size=12),
    )
    fig.update_xaxes(showgrid=False, showline=True,
                     linecolor=C["border"], tickfont=dict(size=11))
    fig.update_yaxes(showgrid=True, gridcolor="#F0F0F0",
                     showline=False, tickfont=dict(size=11))
    return fig

# ── Header ───────────────────────────────────────────────────────
st.markdown(f"""
<div class="dash-header">
    <div>
        <div class="dash-header-title">📊 Adventure Works — Sales Dashboard</div>
        <div class="dash-header-sub">Période : 2011 — 2014 · Adventure Works Cycles</div>
    </div>
    <div class="dash-header-sub">Données mises à jour le 30/03/2026</div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar filters ──────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"### 🎛 Filtres")
    st.markdown("---")
    years = sorted(fs["year"].dropna().unique().astype(int).tolist())
    selected_years = st.multiselect("📅 Année", years, default=years)
    st.markdown("---")
    page = st.radio("📄 Page", [
        "📈 Executive Summary",
        "📦 Produits & Clients",
        "🌍 Territoire & Opérations"
    ])

# ── Filtres appliqués ────────────────────────────────────────────
if not selected_years:
    selected_years = years

fs_f = fs[fs["year"].isin(selected_years)]
fh_f = fh[fh["order_date_key"].astype(str).str[:4].astype(int).isin(selected_years)]
fh_f = fh_f.copy()

# ════════════════════════════════════════════════════════════════
# PAGE 1 — EXECUTIVE SUMMARY
# ════════════════════════════════════════════════════════════════
if page == "📈 Executive Summary":

    ca_total  = fs_f["line_total"].sum()
    nb_cmd    = fh_f["sales_order_id"].nunique()
    marge_pct = (fs_f["margin"].sum() / ca_total * 100) if ca_total > 0 else 0
    aov       = ca_total / nb_cmd if nb_cmd > 0 else 0
    delai_moy = fh_f["days_to_ship"].mean()

    st.markdown(f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-icon">💰</div>
            <div class="kpi-value">{ca_total/1e6:.2f}M€</div>
            <div class="kpi-label">CA Total</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon">🛒</div>
            <div class="kpi-value">{nb_cmd:,}</div>
            <div class="kpi-label">Nb Commandes</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon">📊</div>
            <div class="kpi-value">{marge_pct:.2f}%</div>
            <div class="kpi-label">Marge %</div>
            <div class="kpi-delta {'kpi-delta-pos' if marge_pct > 0 else 'kpi-delta-neg'}">
                {'▲' if marge_pct > 0 else '▼'} {abs(marge_pct):.1f}%
            </div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon">🎯</div>
            <div class="kpi-value">{aov:,.0f}€</div>
            <div class="kpi-label">AOV</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-icon">🚚</div>
            <div class="kpi-value">{delai_moy:.1f}j</div>
            <div class="kpi-label">Délai Moyen</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">📈 Tendances temporelles</div>',
                unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        ca_mois = fs_f.groupby(["year","month"])["line_total"].sum().reset_index()
        ca_mois = ca_mois[ca_mois["line_total"] > 0].sort_values(["year","month"])
        ca_mois["mois_label"] = ca_mois["month"].map(mois_fr)
        fig1 = px.line(
            ca_mois, x="mois_label", y="line_total", color="year",
            labels={"line_total":"CA (€)", "mois_label":"Mois", "year":"Année"},
            color_discrete_sequence=[C["light"], C["secondary"], C["primary"], C["orange"]],
            category_orders={"mois_label": list(mois_fr.values())},
            markers=True
        )
        chart_layout(fig1, "Évolution du CA par Mois")
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">🌍 Répartition géographique & Canal</div>',
                unsafe_allow_html=True)

    col1, col2 = st.columns([3, 2], gap="medium")

    with col1:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        ca_terr = fs_f.groupby("territory_name")["line_total"].sum()\
                      .reset_index().sort_values("line_total", ascending=True)
        fig2 = px.bar(
            ca_terr, x="line_total", y="territory_name", orientation="h",
            labels={"line_total":"Montant (€)", "territory_name":""},
            color_discrete_sequence=[C["primary"]],
            text="line_total"
        )
        fig2.update_traces(texttemplate="%{text:,.0f}€", textposition="outside",
                           marker_color=C["primary"])
        chart_layout(fig2, "CA par Territoire")
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        fh_f["Canal"] = fh_f["online_order_flag"].apply(
            lambda x: "En ligne" if str(x).lower() in ["true","1"] else "En magasin"
        )
        canal = fh_f.groupby("Canal")["sales_order_id"].nunique().reset_index()
        fig3 = px.pie(
            canal, names="Canal", values="sales_order_id", hole=0.55,
            color_discrete_sequence=[C["primary"], C["light"]]
        )
        fig3.update_traces(textinfo="percent+label",
                           textfont=dict(size=12),
                           marker=dict(line=dict(color=C["white"], width=2)))
        chart_layout(fig3, "Online vs Offline")
        fig3.update_layout(showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# PAGE 2 — PRODUITS & CLIENTS
# ════════════════════════════════════════════════════════════════
elif page == "📦 Produits & Clients":

    st.markdown('<div class="section-title">📦 Analyse Produits</div>',
                unsafe_allow_html=True)

    col1, col2 = st.columns(2, gap="medium")

    with col1:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        top_prod = (
            fs_f.groupby("product_name")["line_total"].sum()
            .reset_index().nlargest(10, "line_total")
            .sort_values("line_total", ascending=True)
        )
        fig4 = px.bar(
            top_prod, x="line_total", y="product_name", orientation="h",
            labels={"line_total":"CA (€)", "product_name":""},
            text="line_total"
        )
        fig4.update_traces(texttemplate="%{text:,.0f}€", textposition="outside",
                           marker_color=C["primary"])
        chart_layout(fig4, "Top 10 Produits par CA")
        st.plotly_chart(fig4, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        qty_cat = (
            fs_f.groupby("category")["order_qty"].sum()
            .reset_index().sort_values("order_qty", ascending=False)
        )
        fig5 = px.bar(
            qty_cat, x="category", y="order_qty",
            labels={"order_qty":"Quantité", "category":""},
            text="order_qty",
            color="order_qty",
            color_continuous_scale=[[0, C["light"]], [1, C["accent"]]]
        )
        fig5.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig5.update_layout(coloraxis_showscale=False)
        chart_layout(fig5, "Quantité Vendue par Catégorie")
        st.plotly_chart(fig5, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">👤 Analyse Clients</div>',
                unsafe_allow_html=True)

    col3, col4 = st.columns(2, gap="medium")

    with col3:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        top_clients = (
            fs_f.groupby("full_name").agg(
                CA_Total=("line_total","sum"),
                Quantite=("order_qty","sum"),
                Marge_brute=("margin","sum"),
                CA_sum=("line_total","sum")
            ).reset_index()
        )
        top_clients["Marge_%"] = (
            top_clients["Marge_brute"] / top_clients["CA_sum"] * 100
        ).round(2).astype(str) + "%"
        top_clients = top_clients.nlargest(10, "CA_Total")
        top_clients["CA_Total"] = top_clients["CA_Total"].map("{:,.0f}€".format)
        top_clients = top_clients[["full_name","CA_Total","Quantite","Marge_%"]]
        top_clients.columns = ["Client","CA Total","Quantité","Marge %"]
        st.markdown("**Top 10 Clients**")
        st.dataframe(top_clients, use_container_width=True, hide_index=True,
                     height=380)
        st.markdown('</div>', unsafe_allow_html=True)

    with col4:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        ca_marge = fs_f.groupby("category").agg(
            CA_Total=("line_total","sum"),
            Marge_Brute=("margin","sum")
        ).reset_index().sort_values("CA_Total", ascending=False)
        fig6 = go.Figure()
        fig6.add_trace(go.Bar(
            name="CA Total", x=ca_marge["category"], y=ca_marge["CA_Total"],
            marker_color=C["primary"],
            text=ca_marge["CA_Total"],
            texttemplate="%{text:,.0f}€", textposition="outside"
        ))
        fig6.add_trace(go.Bar(
            name="Marge Brute", x=ca_marge["category"], y=ca_marge["Marge_Brute"],
            marker_color=C["secondary"],
            text=ca_marge["Marge_Brute"],
            texttemplate="%{text:,.0f}€", textposition="outside"
        ))
        fig6.update_layout(barmode="group")
        chart_layout(fig6, "CA vs Marge Brute par Catégorie")
        st.plotly_chart(fig6, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# PAGE 3 — TERRITOIRE & OPÉRATIONS
# ════════════════════════════════════════════════════════════════
elif page == "🌍 Territoire & Opérations":

    st.markdown('<div class="section-title">🌍 Performance Géographique</div>',
                unsafe_allow_html=True)

    col1, col2 = st.columns(2, gap="medium")

    with col1:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        ca_cont = (
            fs_f.groupby("continent")["line_total"].sum()
            .reset_index().sort_values("line_total", ascending=False)
        )
        fig7 = px.bar(
            ca_cont, x="continent", y="line_total",
            labels={"line_total":"CA (€)", "continent":""},
            text="line_total",
            color="line_total",
            color_continuous_scale=[[0, C["light"]], [1, C["accent"]]]
        )
        fig7.update_traces(texttemplate="%{text:,.0f}€", textposition="outside")
        fig7.update_layout(coloraxis_showscale=False)
        chart_layout(fig7, "CA par Continent")
        st.plotly_chart(fig7, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        ca_terr_y = (
            fs_f.groupby(["territory_name","year"])["line_total"]
            .sum().reset_index().sort_values(["territory_name","year"])
        )
        ca_terr_y["prev"] = ca_terr_y.groupby("territory_name")["line_total"].shift(1)
        ca_terr_y["YoY"] = (
            (ca_terr_y["line_total"] - ca_terr_y["prev"]) / ca_terr_y["prev"] * 100
        ).round(1)
        ca_last = (
            ca_terr_y[ca_terr_y["year"] == ca_terr_y["year"].max()]
            .sort_values("line_total", ascending=False)
        )
        fig8 = go.Figure()
        fig8.add_trace(go.Bar(
            name="CA Total", x=ca_last["territory_name"], y=ca_last["line_total"],
            marker_color=C["primary"], yaxis="y1",
            text=ca_last["line_total"],
            texttemplate="%{text:,.0f}€", textposition="outside"
        ))
        fig8.add_trace(go.Scatter(
            name="YoY %", x=ca_last["territory_name"], y=ca_last["YoY"],
            mode="lines+markers", line=dict(color=C["orange"], width=2),
            marker=dict(size=7), yaxis="y2"
        ))
        fig8.update_layout(
            yaxis=dict(title="CA (€)", showgrid=True, gridcolor="#F0F0F0"),
            yaxis2=dict(title="YoY %", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis_title=""
        )
        chart_layout(fig8, "CA et Croissance YoY par Territoire")
        st.plotly_chart(fig8, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">🚚 Opérations & Livraison</div>',
                unsafe_allow_html=True)

    col3, col4 = st.columns(2, gap="medium")

    with col3:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        delai = (
            fh_f.groupby("ship_method_name")["days_to_ship"]
            .mean().reset_index().sort_values("days_to_ship", ascending=True)
        )
        delai["days_to_ship"] = delai["days_to_ship"].round(1)
        fig9 = px.bar(
            delai, x="days_to_ship", y="ship_method_name", orientation="h",
            labels={"days_to_ship":"Délai (jours)", "ship_method_name":""},
            text="days_to_ship"
        )
        fig9.update_traces(texttemplate="%{text:.1f}j", textposition="outside",
                           marker_color=C["secondary"])
        chart_layout(fig9, "Délai Moyen par Méthode d'Expédition")
        st.plotly_chart(fig9, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col4:
        st.markdown('<div class="charts-container">', unsafe_allow_html=True)
        fh_f["online"] = fh_f["online_order_flag"].apply(
            lambda x: 1 if str(x).lower() in ["true","1"] else 0
        )
        online_terr = fh_f.groupby("territory_name").agg(
            total=("sales_order_id","count"),
            online=("online","sum")
        ).reset_index()
        online_terr["pct_online"] = (
            online_terr["online"] / online_terr["total"] * 100
        ).round(2)
        online_terr = online_terr.sort_values("pct_online", ascending=True)
        fig10 = px.bar(
            online_terr, x="pct_online", y="territory_name", orientation="h",
            labels={"pct_online":"% En Ligne", "territory_name":""},
            text="pct_online",
            color="pct_online",
            color_continuous_scale=[[0, C["light"]], [1, C["accent"]]]
        )
        fig10.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig10.update_layout(coloraxis_showscale=False)
        chart_layout(fig10, "% Commandes En Ligne par Territoire")
        st.plotly_chart(fig10, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)