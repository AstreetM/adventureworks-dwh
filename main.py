# main.py
from config import SOURCE_DB, DWH_DB
from etl.utils import get_engine, log
from etl.extract import extract_all
from etl.validate import validate_all
from etl.transform import (
    build_dim_date, build_dim_territory, build_dim_shipmethod,
    build_dim_salesperson, build_dim_product, build_dim_customer,
    build_fact_sales_header, build_fact_sales
)
from etl.load import (
    load_static_dim, load_dim_date, load_scd2,
    load_fact_sales_header, load_fact_sales
)
import pandas as pd
from sqlalchemy import text


def read_dim(table: str, cols: list, dwh_engine) -> pd.DataFrame:
    """Relit une dimension depuis le DWH après chargement."""
    with dwh_engine.connect() as conn:
        return pd.read_sql(
            text(f"SELECT {', '.join(cols)} FROM {table} WHERE is_current = 1"),
            conn
        )

def read_dim_static(table: str, cols: list, dwh_engine) -> pd.DataFrame:
    """Relit une dimension statique depuis le DWH après chargement."""
    with dwh_engine.connect() as conn:
        return pd.read_sql(
            text(f"SELECT {', '.join(cols)} FROM {table}"),
            conn
        )


def run():
    log("=" * 60)
    log("🚀 DÉMARRAGE ETL AdventureWorks → AdventureWorksDWH")
    log("=" * 60)

    # ── Connexions ────────────────────────────────────────────────
    log("[INIT] Connexion aux bases de données...")
    source_engine = get_engine(SOURCE_DB)
    dwh_engine    = get_engine(DWH_DB)
    log("[INIT] ✅ Connexions établies")

    # ── EXTRACT ───────────────────────────────────────────────────
    log("")
    log("── ÉTAPE 1 : EXTRACTION ──────────────────────────────────")
    data = extract_all(source_engine)
    log("[EXTRACT] Résumé :")
    for name, df in data.items():
        log(f"  {name:<20} → {len(df):>7} lignes")

    # ── VALIDATE ──────────────────────────────────────────────────
    log("")
    log("── ÉTAPE 2 : VALIDATION ──────────────────────────────────")
    data = validate_all(data)
    log("[VALIDATE] Résumé après nettoyage :")
    for name, df in data.items():
        log(f"  {name:<20} → {len(df):>7} lignes")

    # ── TRANSFORM + LOAD DIMENSIONS ───────────────────────────────
    log("")
    log("── ÉTAPE 3 : TRANSFORM + LOAD DIMENSIONS ─────────────────")

    # DimDate
    dim_date = build_dim_date(data)
    load_dim_date(dim_date, dwh_engine)

    # DimTerritory
    dim_territory = build_dim_territory(data)
    load_static_dim(dim_territory, "DimTerritory", "territory_id", dwh_engine)

    # DimShipMethod
    dim_shipmethod = build_dim_shipmethod(data)
    load_static_dim(dim_shipmethod, "DimShipMethod", "shipmethod_id", dwh_engine)

    # DimSalesPerson
    dim_salesperson = build_dim_salesperson(data)
    load_scd2(
        dim_salesperson, "DimSalesPerson", "salesperson_id",
        ["full_name", "job_title", "territory_id"], dwh_engine
    )

    # DimProduct
    dim_product = build_dim_product(data)
    load_scd2(
        dim_product, "DimProduct", "product_id",
        ["product_name", "list_price", "standard_cost",
         "category", "subcategory", "color"], dwh_engine
    )

    # DimCustomer
    dim_customer = build_dim_customer(data)
    load_scd2(
        dim_customer, "DimCustomer", "customer_id",
        ["full_name", "customer_type", "city", "state", "country"], dwh_engine
    )

    # ── RELIRE LES DIMS DEPUIS LA DB (avec surrogate keys) ────────
    log("")
    log("── ÉTAPE 4 : RELECTURE DIMENSIONS DEPUIS DWH ─────────────")
    dim_customer_db    = read_dim("DimCustomer",   ["customer_key",   "customer_id"],   dwh_engine)
    dim_product_db     = read_dim("DimProduct",    ["product_key",    "product_id"],    dwh_engine)
    dim_salesperson_db = read_dim("DimSalesPerson",["salesperson_key","salesperson_id"],dwh_engine)
    dim_territory_db   = read_dim_static("DimTerritory",  ["territory_key",  "territory_id"],  dwh_engine)
    dim_shipmethod_db  = read_dim_static("DimShipMethod", ["shipmethod_key", "shipmethod_id"], dwh_engine)

    log(f"  DimCustomer    → {len(dim_customer_db)} lignes relues")
    log(f"  DimProduct     → {len(dim_product_db)} lignes relues")
    log(f"  DimSalesPerson → {len(dim_salesperson_db)} lignes relues")
    log(f"  DimTerritory   → {len(dim_territory_db)} lignes relues")
    log(f"  DimShipMethod  → {len(dim_shipmethod_db)} lignes relues")

    # ── TRANSFORM + LOAD FAITS ────────────────────────────────────
    log("")
    log("── ÉTAPE 5 : TRANSFORM + LOAD FAITS ──────────────────────")

    # FactSalesHeader
    fact_header = build_fact_sales_header(
        data,
        dim_customer_db, dim_territory_db,
        dim_salesperson_db, dim_shipmethod_db
    )
    load_fact_sales_header(fact_header, dwh_engine)

    # FactSales
    fact_sales = build_fact_sales(
        data,
        dim_product_db, dim_customer_db,
        dim_territory_db, dim_salesperson_db, dim_shipmethod_db
    )
    load_fact_sales(fact_sales, dwh_engine)

    log("")
    log("=" * 60)
    log("✅ ETL TERMINÉ AVEC SUCCÈS")
    log("=" * 60)


if __name__ == "__main__":
    run()