import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Config ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Adventure Works — Sales Dashboard",
    page_icon="📊",
    layout="wide"
)

# ── CSS custom ───────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #F8F9FA; }
    .kpi-card {
        background: white;
        border-radius: 8px;
        padding: 20px;
        border: 1px solid #D6DCE4;
        box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        text-align: center;
    }
    .kpi-value { font-size: 28px; font-weight: bold; color: #1F4E79; }
    .kpi-label { font-size: 13px; color: #595959; margin-top: 4px; }
    .header-bar {
        background: #1F4E79;
        padding: 16px 24px;
        border-radius: 8px;
        margin-bottom: 24px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .header-title { color: white; font-size: 22px; font-weight: bold; }
    .header-sub { color: #BDD7EE; font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# ── Chargement données ───────────────────────────────────────────
BASE_URL = "https://raw.githubusercontent.com/AstreetM/adventureworks-dwh/main/data/csv/"

@st.cache_data
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

# ── Jointures de base ────────────────────────────────────────────
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

# ── Mapping mois FR ──────────────────────────────────────────────
mois_fr = {
    1:"Janvier", 2:"Février", 3:"Mars", 4:"Avril",
    5:"Mai", 6:"Juin", 7:"Juillet", 8:"Août",
    9:"Septembre", 10:"Octobre", 11:"Novembre", 12:"Décembre"
}
fs["mois_label"] = fs["month"].map(mois_fr)

# ── Header ───────────────────────────────────────────────────────
st.markdown("""
<div class="header-bar">
    <span class="header-title">📊 Adventure Works — Sales Dashboard</span>
    <span class="header-sub">Période : 2011 — 2014</span>
</div>
""", unsafe_allow_html=True)

# ── Navigation & Slicer ──────────────────────────────────────────
page = st.sidebar.radio("Navigation", [
    "📈 Executive Summary",
    "📦 Produits & Clients",
    "🌍 Territoire & Opérations"
])

years = sorted(fs["year"].dropna().unique().astype(int).tolist())
selected_years = st.sidebar.multiselect("Année", years, default=years)

# ── Filtres ──────────────────────────────────────────────────────
fs_f = fs[fs["year"].isin(selected_years)]
fh_f = fh[fh["order_date_key"].astype(str).str[:4].astype(int).isin(selected_years)]

# ════════════════════════════════════════════════════════════════
# PAGE 1 — EXECUTIVE SUMMARY
# ════════════════════════════════════════════════════════════════
if page == "📈 Executive Summary":

    # KPI Cards
    ca_total  = fs_f["line_total"].sum()
    nb_cmd    = fh_f["sales_order_id"].nunique()
    marge_pct = (fs_f["margin"].sum() / ca_total * 100) if ca_total > 0 else 0
    aov       = ca_total / nb_cmd if nb_cmd > 0 else 0
    delai_moy = fh_f["days_to_ship"].mean()

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-value">{ca_total/1e6:.2f}M€</div>
            <div class="kpi-label">CA Total</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-value">{nb_cmd:,}</div>
            <div class="kpi-label">Nb Commandes</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-value">{marge_pct:.2f}%</div>
            <div class="kpi-label">Marge %</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-value">{aov:,.0f}€</div>
            <div class="kpi-label">AOV</div></div>""", unsafe_allow_html=True)
    with c5:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-value">{delai_moy:.1f}j</div>
            <div class="kpi-label">Délai Moyen</div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Courbe CA par mois — triée par numéro de mois
    ca_mois = fs_f.groupby(["year","month"])["line_total"].sum().reset_index()
    ca_mois = ca_mois.sort_values(["year","month"])
    ca_mois["mois_label"] = ca_mois["month"].map(mois_fr)

    fig1 = px.line(
        ca_mois, x="mois_label", y="line_total", color="year",
        title="Évolution du CA par Mois",
        labels={"line_total":"CA Total (€)", "mois_label":"Mois", "year":"Année"},
        color_discrete_sequence=["#BDD7EE","#2E75B6","#1F4E79","#F0A500"],
        category_orders={"mois_label": list(mois_fr.values())}
    )
    fig1.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        font_color="#595959", title_font_color="#1F4E79"
    )
    fig1.update_xaxes(showgrid=False)
    fig1.update_yaxes(showgrid=True, gridcolor="#F0F0F0")
    st.plotly_chart(fig1, use_container_width=True)

    col1, col2 = st.columns(2)

    # Barres CA par Territoire — trié décroissant
    with col1:
        ca_terr = fs_f.groupby("territory_name")["line_total"].sum().reset_index()
        ca_terr = ca_terr.sort_values("line_total", ascending=True)
        fig2 = px.bar(
            ca_terr, x="line_total", y="territory_name", orientation="h",
            title="CA par Territoire",
            labels={"line_total":"Montant (€)", "territory_name":"Territoire"},
            color_discrete_sequence=["#2E75B6"],
            text="line_total"
        )
        fig2.update_traces(texttemplate="%{text:,.0f}€", textposition="outside")
        fig2.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#595959", title_font_color="#1F4E79"
        )
        fig2.update_xaxes(showgrid=True, gridcolor="#F0F0F0")
        fig2.update_yaxes(showgrid=False)
        st.plotly_chart(fig2, use_container_width=True)

    # Donut Online vs Offline
    with col2:
        fh_f = fh_f.copy()
        fh_f["Canal"] = fh_f["online_order_flag"].apply(
            lambda x: "En ligne" if str(x).lower() in ["true","1"] else "En magasin"
        )
        canal = fh_f.groupby("Canal")["sales_order_id"].nunique().reset_index()
        fig3 = px.pie(
            canal, names="Canal", values="sales_order_id", hole=0.5,
            title="Commandes Online vs Offline",
            color_discrete_sequence=["#1F4E79","#BDD7EE"]
        )
        fig3.update_traces(textinfo="percent+label")
        fig3.update_layout(
            paper_bgcolor="white", font_color="#595959",
            title_font_color="#1F4E79"
        )
        st.plotly_chart(fig3, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# PAGE 2 — PRODUITS & CLIENTS
# ════════════════════════════════════════════════════════════════
elif page == "📦 Produits & Clients":

    col1, col2 = st.columns(2)

    # Top 10 Produits — trié décroissant
    with col1:
        top_prod = (
            fs_f.groupby("product_name")["line_total"].sum()
            .reset_index()
            .nlargest(10, "line_total")
            .sort_values("line_total", ascending=True)
        )
        fig4 = px.bar(
            top_prod, x="line_total", y="product_name", orientation="h",
            title="Top 10 Produits par CA",
            labels={"line_total":"CA Total (€)", "product_name":"Produit"},
            color_discrete_sequence=["#2E75B6"],
            text="line_total"
        )
        fig4.update_traces(texttemplate="%{text:,.0f}€", textposition="outside")
        fig4.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#595959", title_font_color="#1F4E79"
        )
        st.plotly_chart(fig4, use_container_width=True)

    # Quantité vendue par catégorie — trié décroissant
    with col2:
        qty_cat = (
            fs_f.groupby("category")["order_qty"].sum()
            .reset_index()
            .sort_values("order_qty", ascending=False)
        )
        fig5 = px.bar(
            qty_cat, x="category", y="order_qty",
            title="Quantité Vendue par Catégorie",
            labels={"order_qty":"Quantité", "category":"Catégorie"},
            color_discrete_sequence=["#2E75B6"],
            text="order_qty"
        )
        fig5.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig5.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#595959", title_font_color="#1F4E79"
        )
        st.plotly_chart(fig5, use_container_width=True)

    col3, col4 = st.columns(2)

    # Top 10 Clients — trié par CA décroissant
    with col3:
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
        top_clients["CA_Total"] = top_clients["CA_Total"].round(2)
        top_clients = top_clients[["full_name","CA_Total","Quantite","Marge_%"]]
        top_clients.columns = ["Client","CA Total (€)","Quantité","Marge %"]
        st.markdown("#### Top 10 Clients")
        st.dataframe(top_clients, use_container_width=True, hide_index=True)

    # CA vs Marge Brute par Catégorie
    with col4:
        ca_marge = fs_f.groupby("category").agg(
            CA_Total=("line_total","sum"),
            Marge_Brute=("margin","sum")
        ).reset_index().sort_values("CA_Total", ascending=False)

        fig6 = go.Figure()
        fig6.add_trace(go.Bar(
            name="CA Total", x=ca_marge["category"],
            y=ca_marge["CA_Total"], marker_color="#1F4E79",
            text=ca_marge["CA_Total"],
            texttemplate="%{text:,.0f}€", textposition="outside"
        ))
        fig6.add_trace(go.Bar(
            name="Marge Brute", x=ca_marge["category"],
            y=ca_marge["Marge_Brute"], marker_color="#BDD7EE",
            text=ca_marge["Marge_Brute"],
            texttemplate="%{text:,.0f}€", textposition="outside"
        ))
        fig6.update_layout(
            title="CA vs Marge Brute par Catégorie",
            barmode="group",
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#595959", title_font_color="#1F4E79",
            yaxis_title="Montant (€)", xaxis_title="Catégorie"
        )
        st.plotly_chart(fig6, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# PAGE 3 — TERRITOIRE & OPÉRATIONS
# ════════════════════════════════════════════════════════════════
elif page == "🌍 Territoire & Opérations":

    col1, col2 = st.columns(2)

    # CA par Continent — trié décroissant
    with col1:
        ca_cont = (
            fs_f.groupby("continent")["line_total"].sum()
            .reset_index()
            .sort_values("line_total", ascending=False)
        )
        fig7 = px.bar(
            ca_cont, x="continent", y="line_total",
            title="CA par Continent",
            labels={"line_total":"CA Total (€)", "continent":"Continent"},
            color_discrete_sequence=["#2E75B6"],
            text="line_total"
        )
        fig7.update_traces(texttemplate="%{text:,.0f}€", textposition="outside")
        fig7.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#595959", title_font_color="#1F4E79"
        )
        st.plotly_chart(fig7, use_container_width=True)

    # CA et Croissance YoY par Territoire — graphique combiné barres + courbe
    with col2:
        ca_terr_y = (
            fs_f.groupby(["territory_name","year"])["line_total"]
            .sum().reset_index()
            .sort_values(["territory_name","year"])
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
            name="CA Total",
            x=ca_last["territory_name"],
            y=ca_last["line_total"],
            marker_color="#1F4E79",
            yaxis="y1",
            text=ca_last["line_total"],
            texttemplate="%{text:,.0f}€",
            textposition="outside"
        ))
        fig8.add_trace(go.Scatter(
            name="CA Croissance YoY %",
            x=ca_last["territory_name"],
            y=ca_last["YoY"],
            mode="lines+markers",
            marker_color="#F0A500",
            yaxis="y2"
        ))
        fig8.update_layout(
            title="CA et Croissance YoY par Territoire",
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#595959", title_font_color="#1F4E79",
            yaxis=dict(title="CA (€)", showgrid=True, gridcolor="#F0F0F0"),
            yaxis2=dict(title="YoY %", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis_title="Territoire"
        )
        st.plotly_chart(fig8, use_container_width=True)

    col3, col4 = st.columns(2)

    # Délai moyen par méthode
    with col3:
        delai = (
            fh_f.groupby("ship_method_name")["days_to_ship"]
            .mean().reset_index()
            .sort_values("days_to_ship", ascending=True)
        )
        delai["days_to_ship"] = delai["days_to_ship"].round(1)
        fig9 = px.bar(
            delai, x="days_to_ship", y="ship_method_name", orientation="h",
            title="Délai Moyen par Méthode d'Expédition",
            labels={"days_to_ship":"Délai Moyen (jours)", "ship_method_name":"Méthode"},
            color_discrete_sequence=["#2E75B6"],
            text="days_to_ship"
        )
        fig9.update_traces(texttemplate="%{text:.1f}j", textposition="outside")
        fig9.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#595959", title_font_color="#1F4E79"
        )
        st.plotly_chart(fig9, use_container_width=True)

    # % Commandes En Ligne par Territoire — trié décroissant
    with col4:
        fh_f = fh_f.copy()
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
            title="% Commandes En Ligne par Territoire",
            labels={"pct_online":"% En Ligne", "territory_name":"Territoire"},
            color_discrete_sequence=["#2E75B6"],
            text="pct_online"
        )
        fig10.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig10.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            font_color="#595959", title_font_color="#1F4E79"
        )
        st.plotly_chart(fig10, use_container_width=True)