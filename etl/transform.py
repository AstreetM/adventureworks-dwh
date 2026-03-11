# etl/transform.py
import pandas as pd
import numpy as np
from datetime import datetime, date
from etl.utils import log


# ── DimDate ───────────────────────────────────────────────────────────────────
def build_dim_date(data: dict) -> pd.DataFrame:
    """
    Génère DimDate à partir de toutes les dates présentes
    dans fact_sales et fact_sales_header.
    Pas de table source — on génère en Python.
    """
    log("[TRANSFORM] Construction DimDate...")

    dates = set()
    for df_name in ["fact_sales", "fact_sales_header"]:
        df = data[df_name]
        for col in ["order_date", "ship_date", "due_date"]:
            if col in df.columns:
                dates.update(df[col].dropna().dt.date.unique())

    rows = []
    for d in sorted(dates):
        dt = datetime.combine(d, datetime.min.time())
        rows.append({
            "date_key":    int(dt.strftime("%Y%m%d")),
            "full_date":   d,
            "year":        dt.year,
            "quarter":     (dt.month - 1) // 3 + 1,
            "month":       dt.month,
            "month_name":  dt.strftime("%B"),
            "day":         dt.day,
            "day_of_week": dt.isoweekday(),
            "day_name":    dt.strftime("%A"),
            "is_weekend":  1 if dt.isoweekday() >= 6 else 0,
        })

    dim_date = pd.DataFrame(rows).drop_duplicates(subset=["date_key"])
    log(f"[TRANSFORM] DimDate → {len(dim_date)} dates")
    return dim_date


# ── DimTerritory ──────────────────────────────────────────────────────────────
def build_dim_territory(data: dict) -> pd.DataFrame:
    log("[TRANSFORM] Construction DimTerritory...")
    df = data["dim_territory"].copy()
    df = df.drop_duplicates(subset=["territory_id"])
    log(f"[TRANSFORM] DimTerritory → {len(df)} lignes")
    return df


# ── DimShipMethod ─────────────────────────────────────────────────────────────
def build_dim_shipmethod(data: dict) -> pd.DataFrame:
    log("[TRANSFORM] Construction DimShipMethod...")
    df = data["dim_shipmethod"].copy()
    df = df.drop_duplicates(subset=["shipmethod_id"])
    log(f"[TRANSFORM] DimShipMethod → {len(df)} lignes")
    return df


# ── DimSalesPerson ────────────────────────────────────────────────────────────
def build_dim_salesperson(data: dict) -> pd.DataFrame:
    log("[TRANSFORM] Construction DimSalesPerson...")
    df = data["dim_salesperson"].copy()
    df = df.drop_duplicates(subset=["salesperson_id"])
    log(f"[TRANSFORM] DimSalesPerson → {len(df)} lignes")
    return df


# ── DimProduct ────────────────────────────────────────────────────────────────
def build_dim_product(data: dict) -> pd.DataFrame:
    log("[TRANSFORM] Construction DimProduct...")
    df = data["dim_product"].copy()

    df["category"]    = df["category"].fillna("Uncategorized")
    df["subcategory"] = df["subcategory"].fillna("Uncategorized")
    df["color"]       = df["color"].fillna("N/A")
    df["size"]        = df["size"].fillna("N/A")
    df["weight"]      = df["weight"].fillna(0)

    df = df.drop_duplicates(subset=["product_id"])
    log(f"[TRANSFORM] DimProduct → {len(df)} lignes")
    return df


# ── DimCustomer ───────────────────────────────────────────────────────────────
def build_dim_customer(data: dict) -> pd.DataFrame:
    log("[TRANSFORM] Construction DimCustomer...")
    df = data["dim_customer"].copy()

    for col in ["city", "state", "country"]:
        df[col] = df[col].fillna("Unknown")

    df = df.drop_duplicates(subset=["customer_id"])
    log(f"[TRANSFORM] DimCustomer → {len(df)} lignes")
    return df


# ── Helper : date → date_key ──────────────────────────────────────────────────
def date_to_key(date_val) -> int:
    """Convertit une date en date_key (YYYYMMDD)."""
    if pd.isna(date_val):
        return 19000101  # date inconnue = clé par défaut
    if isinstance(date_val, (datetime, pd.Timestamp)):
        return int(date_val.strftime("%Y%m%d"))
    return int(pd.Timestamp(date_val).strftime("%Y%m%d"))


# ── FactSalesHeader ───────────────────────────────────────────────────────────
def build_fact_sales_header(
    data: dict,
    dim_customer: pd.DataFrame,
    dim_territory: pd.DataFrame,
    dim_salesperson: pd.DataFrame,
    dim_shipmethod: pd.DataFrame,
) -> pd.DataFrame:
    log("[TRANSFORM] Construction FactSalesHeader...")
    df = data["fact_sales_header"].copy()

    # Convertir dates en date_key
    df["order_date_key"] = df["order_date"].apply(date_to_key)
    df["ship_date_key"]  = df["ship_date"].apply(date_to_key)
    df["due_date_key"]   = df["due_date"].apply(date_to_key)

    # Lookup surrogate keys
    df = df.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id", how="left"
    )
    df = df.merge(
        dim_territory[["territory_key", "territory_id"]],
        on="territory_id", how="left"
    )
    df = df.merge(
        dim_shipmethod[["shipmethod_key", "shipmethod_id"]],
        on="shipmethod_id", how="left"
    )
    df = df.merge(
        dim_salesperson[["salesperson_key", "salesperson_id"]],
        on="salesperson_id", how="left"
    )

    cols = [
        "sales_order_id", "customer_key", "territory_key",
        "salesperson_key", "shipmethod_key",
        "order_date_key", "ship_date_key", "due_date_key",
        "subtotal", "tax_amount", "freight", "total_due",
        "days_to_ship", "online_order_flag", "order_number"
    ]
    fact = df[cols].drop_duplicates(subset=["sales_order_id"])
    log(f"[TRANSFORM] FactSalesHeader → {len(fact)} lignes")
    return fact


# ── FactSales ─────────────────────────────────────────────────────────────────
def build_fact_sales(
    data: dict,
    dim_product: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_territory: pd.DataFrame,
    dim_salesperson: pd.DataFrame,
    dim_shipmethod: pd.DataFrame,
) -> pd.DataFrame:
    log("[TRANSFORM] Construction FactSales...")
    df = data["fact_sales"].copy()

    # Convertir dates en date_key
    df["order_date_key"] = df["order_date"].apply(date_to_key)
    df["ship_date_key"]  = df["ship_date"].apply(date_to_key)
    df["due_date_key"]   = df["due_date"].apply(date_to_key)

    # Lookup surrogate keys
    df = df.merge(
        dim_product[["product_key", "product_id"]],
        on="product_id", how="left"
    )
    df = df.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id", how="left"
    )
    df = df.merge(
        dim_territory[["territory_key", "territory_id"]],
        on="territory_id", how="left"
    )
    df = df.merge(
        dim_shipmethod[["shipmethod_key", "shipmethod_id"]],
        on="shipmethod_id", how="left"
    )
    df = df.merge(
        dim_salesperson[["salesperson_key", "salesperson_id"]],
        on="salesperson_id", how="left"
    )

    cols = [
        "sales_order_detail_id", "sales_order_id",
        "product_key", "customer_key", "territory_key",
        "salesperson_key", "shipmethod_key",
        "order_date_key", "ship_date_key", "due_date_key",
        "order_qty", "unit_price", "unit_price_discount",
        "line_total", "standard_cost", "margin",
        "online_order_flag", "offer_description",
        "discount_pct", "offer_type", "offer_category"
    ]
    fact = df[cols].drop_duplicates(subset=["sales_order_detail_id"])
    log(f"[TRANSFORM] FactSales → {len(fact)} lignes")
    return fact


# ── Point d'entrée principal ──────────────────────────────────────────────────
def transform_all(data: dict) -> dict:
    """
    Construit toutes les tables du DWH à partir
    du dictionnaire de DataFrames validés.
    """
    dim_date        = build_dim_date(data)
    dim_territory   = build_dim_territory(data)
    dim_shipmethod  = build_dim_shipmethod(data)
    dim_salesperson = build_dim_salesperson(data)
    dim_product     = build_dim_product(data)
    dim_customer    = build_dim_customer(data)

    fact_header = build_fact_sales_header(
        data, dim_customer, dim_territory,
        dim_salesperson, dim_shipmethod
    )
    fact_sales = build_fact_sales(
        data, dim_product, dim_customer,
        dim_territory, dim_salesperson, dim_shipmethod
    )

    return {
        "dim_date":          dim_date,
        "dim_territory":     dim_territory,
        "dim_shipmethod":    dim_shipmethod,
        "dim_salesperson":   dim_salesperson,
        "dim_product":       dim_product,
        "dim_customer":      dim_customer,
        "fact_sales_header": fact_header,
        "fact_sales":        fact_sales,
    }