# tests/test_validate.py
import pytest
import pandas as pd
import numpy as np
from etl.validate import validate_and_clean, REQUIRED_COLS, BUSINESS_KEYS, DEDUP_KEYS


# ── Fixtures : données de test ────────────────────────────────────────────────

@pytest.fixture
def valid_dim_territory():
    return pd.DataFrame({
        "territory_id":   [1, 2, 3],
        "territory_name": ["Northwest", "Northeast", "Central"],
        "country_code":   ["US", "US", "US"],
        "continent":      ["North America", "North America", "North America"]
    })

@pytest.fixture
def valid_dim_customer():
    return pd.DataFrame({
        "customer_id":   [1, 2, 3],
        "full_name":     ["Jean Dupont", "Marie Martin", "Pierre Durand"],
        "customer_type": ["Individual", "Individual", "Store"],
        "city":          ["Paris", None, "Lyon"],
        "state":         ["IDF", None, "ARA"],
        "country":       ["France", None, "France"]
    })

@pytest.fixture
def valid_fact_sales():
    return pd.DataFrame({
        "sales_order_detail_id": [1, 2, 3],
        "sales_order_id":        [100, 100, 101],
        "product_id":            [10, 20, 10],
        "customer_id":           [1, 1, 2],
        "order_qty":             [2, 1, 3],
        "unit_price":            [99.99, 49.99, 99.99],
        "line_total":            [199.98, 49.99, 299.97],
        "order_date":            ["2024-01-01", "2024-01-01", "2024-01-02"],
        "ship_date":             ["2024-01-08", "2024-01-08", "2024-01-09"],
        "due_date":              ["2024-01-13", "2024-01-13", "2024-01-14"],
    })


# ── Tests REQUIRED_COLS ───────────────────────────────────────────────────────

def test_missing_required_column_raises_error(valid_dim_territory):
    """Si une colonne obligatoire manque → ValueError."""
    df = valid_dim_territory.drop(columns=["territory_name"])
    with pytest.raises(ValueError, match="Colonnes manquantes"):
        validate_and_clean("dim_territory", df)

def test_all_required_columns_present(valid_dim_territory):
    """Toutes les colonnes présentes → pas d'erreur."""
    result = validate_and_clean("dim_territory", valid_dim_territory)
    assert result is not None


# ── Tests NULL acceptables ────────────────────────────────────────────────────

def test_null_city_state_country_accepted(valid_dim_customer):
    """NULL dans city/state/country → pas de quarantaine (décision métier)."""
    result = validate_and_clean("dim_customer", valid_dim_customer)
    assert len(result) == 3  # aucune ligne rejetée

def test_null_business_key_rejected(valid_dim_customer):
    """NULL dans customer_id → ligne rejetée en quarantaine."""
    df = valid_dim_customer.copy()
    df.loc[0, "customer_id"] = None
    result = validate_and_clean("dim_customer", df)
    assert len(result) == 2  # 1 ligne rejetée


# ── Tests dédoublonnage ───────────────────────────────────────────────────────

def test_dedup_on_business_key(valid_dim_territory):
    """Doublons sur territory_id → supprimés."""
    df = pd.concat([valid_dim_territory, valid_dim_territory], ignore_index=True)
    assert len(df) == 6
    result = validate_and_clean("dim_territory", df)
    assert len(result) == 3  # doublons supprimés

def test_no_false_dedup(valid_dim_territory):
    """Pas de suppression si les clés sont toutes différentes."""
    result = validate_and_clean("dim_territory", valid_dim_territory)
    assert len(result) == 3


# ── Tests conversion dates ────────────────────────────────────────────────────

def test_dates_converted_to_datetime(valid_fact_sales):
    """order_date/ship_date/due_date → convertis en datetime."""
    result = validate_and_clean("fact_sales", valid_fact_sales)
    assert pd.api.types.is_datetime64_any_dtype(result["order_date"])
    assert pd.api.types.is_datetime64_any_dtype(result["ship_date"])
    assert pd.api.types.is_datetime64_any_dtype(result["due_date"])


# ── Tests quarantaine valeurs impossibles ─────────────────────────────────────

def test_negative_line_total_rejected(valid_fact_sales):
    """line_total négatif → rejeté."""
    df = valid_fact_sales.copy()
    df.loc[0, "line_total"] = -50.0
    result = validate_and_clean("fact_sales", df)
    assert len(result) == 2  # 1 ligne rejetée

def test_zero_order_qty_rejected(valid_fact_sales):
    """order_qty = 0 → rejeté."""
    df = valid_fact_sales.copy()
    df.loc[0, "order_qty"] = 0
    result = validate_and_clean("fact_sales", df)
    assert len(result) == 2  # 1 ligne rejetée

def test_negative_margin_kept(valid_fact_sales):
    """margin négative → conservée (décision métier)."""
    df = valid_fact_sales.copy()
    df["margin"] = [-100.0, 50.0, 30.0]
    result = validate_and_clean("fact_sales", df)
    assert len(result) == 3  # aucune ligne rejetée


# ── Tests chaînes vides ───────────────────────────────────────────────────────

def test_empty_strings_replaced_by_nan(valid_dim_territory):
    """Chaînes vides → remplacées par NaN."""
    df = valid_dim_territory.copy()
    df.loc[0, "territory_name"] = ""
    result = validate_and_clean("dim_territory", df)
    assert pd.isna(result.iloc[0]["territory_name"])