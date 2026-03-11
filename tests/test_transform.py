# tests/test_transform.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from etl.transform import (
    build_dim_date, build_dim_territory, build_dim_shipmethod,
    build_dim_salesperson, build_dim_product, build_dim_customer,
    build_fact_sales_header, build_fact_sales, date_to_key
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_data():
    return {
        "dim_territory": pd.DataFrame({
            "territory_id":   [1, 2],
            "territory_name": ["Northwest", "Northeast"],
            "country_code":   ["US", "US"],
            "continent":      ["North America", "North America"]
        }),
        "dim_shipmethod": pd.DataFrame({
            "shipmethod_id":   [1, 2],
            "ship_method_name": ["TRUCK", "EXPRESS"],
            "ship_base_cost":  [3.95, 9.95],
            "ship_rate":       [0.99, 1.99]
        }),
        "dim_salesperson": pd.DataFrame({
            "salesperson_id": [274, 275],
            "first_name":     ["Stephen", "Michael"],
            "last_name":      ["Jiang", "Blythe"],
            "full_name":      ["Stephen Jiang", "Michael Blythe"],
            "job_title":      ["Sales Manager", "Sales Representative"],
            "territory_id":   [None, 2.0]
        }),
        "dim_product": pd.DataFrame({
            "product_id":     [771, 772],
            "product_name":   ["Mountain-100 Silver, 38", "Mountain-100 Silver, 42"],
            "product_number": ["BK-M82S-38", "BK-M82S-42"],
            "color":          ["Silver", None],
            "size":           ["38", None],
            "weight":         [20.35, None],
            "list_price":     [3399.99, 3399.99],
            "standard_cost":  [1912.15, 1912.15],
            "subcategory":    ["Mountain Bikes", "Mountain Bikes"],
            "category":       ["Bikes", "Bikes"]
        }),
        "dim_customer": pd.DataFrame({
            "customer_id":   [1, 2],
            "full_name":     ["Jean Dupont", "Marie Martin"],
            "customer_type": ["Individual", "Individual"],
            "city":          ["Paris", None],
            "state":         ["IDF", None],
            "country":       ["France", None]
        }),
        "fact_sales_header": pd.DataFrame({
            "sales_order_id":   [43659, 43660],
            "customer_id":      [1, 2],
            "salesperson_id":   [275.0, None],
            "territory_id":     [1, 2],
            "shipmethod_id":    [1, 1],
            "order_date":       pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "ship_date":        pd.to_datetime(["2024-01-08", "2024-01-09"]),
            "due_date":         pd.to_datetime(["2024-01-13", "2024-01-14"]),
            "subtotal":         [1000.0, 500.0],
            "tax_amount":       [100.0, 50.0],
            "freight":          [30.0, 15.0],
            "total_due":        [1130.0, 565.0],
            "days_to_ship":     [7, 7],
            "online_order_flag":[False, True],
            "order_number":     ["SO43659", "SO43660"]
        }),
        "fact_sales": pd.DataFrame({
            "sales_order_detail_id": [1, 2],
            "sales_order_id":        [43659, 43660],
            "product_id":            [771, 772],
            "customer_id":           [1, 2],
            "territory_id":          [1, 2],
            "salesperson_id":        [275.0, None],
            "shipmethod_id":         [1, 1],
            "order_date":            pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "ship_date":             pd.to_datetime(["2024-01-08", "2024-01-09"]),
            "due_date":              pd.to_datetime(["2024-01-13", "2024-01-14"]),
            "order_qty":             [1, 2],
            "unit_price":            [3399.99, 3399.99],
            "unit_price_discount":   [0.0, 0.0],
            "line_total":            [3399.99, 6799.98],
            "standard_cost":         [1912.15, 1912.15],
            "margin":                [1487.84, 2975.68],
            "online_order_flag":     [False, True],
            "offer_description":     ["No Discount", "No Discount"],
            "discount_pct":          [0.0, 0.0],
            "offer_type":            ["No Discount", "No Discount"],
            "offer_category":        ["No Discount", "No Discount"]
        })
    }


# ── Tests date_to_key ─────────────────────────────────────────────────────────

def test_date_to_key_normal():
    """Date normale → YYYYMMDD."""
    d = pd.Timestamp("2024-01-15")
    assert date_to_key(d) == 20240115

def test_date_to_key_null():
    """Date NULL → 19000101 (valeur par défaut)."""
    assert date_to_key(None) == 19000101
    assert date_to_key(np.nan) == 19000101


# ── Tests DimDate ─────────────────────────────────────────────────────────────

def test_dim_date_contient_toutes_les_dates(sample_data):
    """DimDate contient toutes les dates des faits."""
    result = build_dim_date(sample_data)
    assert 20240101 in result["date_key"].values
    assert 20240108 in result["date_key"].values
    assert 20240113 in result["date_key"].values

def test_dim_date_pas_de_doublons(sample_data):
    """Pas de doublons dans date_key."""
    result = build_dim_date(sample_data)
    assert result["date_key"].duplicated().sum() == 0

def test_dim_date_colonnes(sample_data):
    """DimDate contient les bonnes colonnes."""
    result = build_dim_date(sample_data)
    expected_cols = ["date_key", "year", "quarter", "month",
                     "month_name", "day", "day_name", "is_weekend"]
    for col in expected_cols:
        assert col in result.columns

def test_dim_date_weekend(sample_data):
    """is_weekend = 1 pour samedi/dimanche."""
    result = build_dim_date(sample_data)
    # 2024-01-13 est un samedi
    row = result[result["date_key"] == 20240113]
    assert row["is_weekend"].values[0] == 1


# ── Tests DimProduct ──────────────────────────────────────────────────────────

def test_dim_product_fillna_color(sample_data):
    """color NULL → remplacé par N/A."""
    result = build_dim_product(sample_data)
    assert "N/A" in result["color"].values

def test_dim_product_fillna_weight(sample_data):
    """weight NULL → remplacé par 0."""
    result = build_dim_product(sample_data)
    assert 0 in result["weight"].values

def test_dim_product_fillna_size(sample_data):
    """size NULL → remplacé par N/A."""
    result = build_dim_product(sample_data)
    assert "N/A" in result["size"].values

def test_dim_product_pas_de_doublons(sample_data):
    """Pas de doublons sur product_id."""
    result = build_dim_product(sample_data)
    assert result["product_id"].duplicated().sum() == 0


# ── Tests DimCustomer ─────────────────────────────────────────────────────────

def test_dim_customer_fillna_geo(sample_data):
    """city/state/country NULL → remplacé par Unknown."""
    result = build_dim_customer(sample_data)
    assert "Unknown" in result["city"].values
    assert "Unknown" in result["state"].values
    assert "Unknown" in result["country"].values


# ── Tests FactSalesHeader ─────────────────────────────────────────────────────

def test_fact_header_surrogate_keys(sample_data):
    """Les surrogate keys sont bien joints."""
    # Simuler les dims relues depuis le DWH (avec surrogate keys)
    dim_customer    = pd.DataFrame({"customer_key": [10, 20], "customer_id": [1, 2]})
    dim_territory   = pd.DataFrame({"territory_key": [100, 200], "territory_id": [1, 2]})
    dim_salesperson = pd.DataFrame({"salesperson_key": [1000, 2000], "salesperson_id": [274.0, 275.0]})
    dim_shipmethod  = pd.DataFrame({"shipmethod_key": [5, 6], "shipmethod_id": [1, 2]})

    result = build_fact_sales_header(
        sample_data, dim_customer, dim_territory,
        dim_salesperson, dim_shipmethod
    )
    assert "customer_key"   in result.columns
    assert "territory_key"  in result.columns
    assert "shipmethod_key" in result.columns
    assert result.iloc[0]["customer_key"] == 10

def test_fact_header_date_keys(sample_data):
    """Les dates sont bien converties en date_key."""
    dim_customer    = pd.DataFrame({"customer_key": [10, 20], "customer_id": [1, 2]})
    dim_territory   = pd.DataFrame({"territory_key": [100, 200], "territory_id": [1, 2]})
    dim_salesperson = pd.DataFrame({"salesperson_key": [1000, 2000], "salesperson_id": [274.0, 275.0]})
    dim_shipmethod  = pd.DataFrame({"shipmethod_key": [5, 6], "shipmethod_id": [1, 2]})

    result = build_fact_sales_header(
        sample_data, dim_customer, dim_territory,
        dim_salesperson, dim_shipmethod
    )
    assert result.iloc[0]["order_date_key"] == 20240101
    assert result.iloc[0]["ship_date_key"]  == 20240108

def test_fact_header_null_salesperson(sample_data):
    """salesperson_id NULL (commande en ligne) → salesperson_key NULL."""
    dim_customer    = pd.DataFrame({"customer_key": [10, 20], "customer_id": [1, 2]})
    dim_territory   = pd.DataFrame({"territory_key": [100, 200], "territory_id": [1, 2]})
    dim_salesperson = pd.DataFrame({"salesperson_key": [1000, 2000], "salesperson_id": [274.0, 275.0]})
    dim_shipmethod  = pd.DataFrame({"shipmethod_key": [5, 6], "shipmethod_id": [1, 2]})

    result = build_fact_sales_header(
        sample_data, dim_customer, dim_territory,
        dim_salesperson, dim_shipmethod
    )
    # La 2ème ligne a salesperson_id = None → salesperson_key doit être NaN
    assert pd.isna(result.iloc[1]["salesperson_key"])