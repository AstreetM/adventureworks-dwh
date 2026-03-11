# etl/extract.py
import pandas as pd
from etl.utils import get_engine, log

def extract_all(source_engine) -> dict:
    """
    Lit toutes les vues source depuis AdventureWorks2019.
    Retourne un dictionnaire de DataFrames, un par vue.
    """
    views = {
        "dim_territory":     "SELECT * FROM dbo.v_DimTerritory",
        "dim_shipmethod":    "SELECT * FROM dbo.v_DimShipMethod",
        "dim_salesperson":   "SELECT * FROM dbo.v_DimSalesPerson",
        "dim_product":       "SELECT * FROM dbo.v_DimProduct",
        "dim_customer":      "SELECT * FROM dbo.v_DimCustomer",
        "fact_sales_header": "SELECT * FROM dbo.v_FactSalesHeader",
        "fact_sales":        "SELECT * FROM dbo.v_FactSales",
    }

    data = {}
    with source_engine.connect() as conn:
        for name, query in views.items():
            log(f"[EXTRACT] Lecture de {name}...")
            data[name] = pd.read_sql(query, conn)
            log(f"[EXTRACT] {name} → {len(data[name])} lignes")

    return data