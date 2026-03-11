# tests/test_load.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from etl.load import load_static_dim, load_dim_date, load_scd2


# ── Fixture : base SQLite en mémoire ─────────────────────────────────────────

@pytest.fixture
def sqlite_engine():
    """Crée une base SQLite en mémoire pour les tests."""
    engine = create_engine("sqlite:///:memory:")
    return engine


@pytest.fixture
def engine_with_territory(sqlite_engine):
    """SQLite avec table DimTerritory vide."""
    with sqlite_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE DimTerritory (
                territory_key  INTEGER PRIMARY KEY AUTOINCREMENT,
                territory_id   INTEGER UNIQUE,
                territory_name TEXT,
                country_code   TEXT,
                continent      TEXT
            )
        """))
    return sqlite_engine


@pytest.fixture
def engine_with_dimdate(sqlite_engine):
    """SQLite avec table DimDate vide."""
    with sqlite_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE DimDate (
                date_key    INTEGER PRIMARY KEY,
                full_date   TEXT,
                year        INTEGER,
                quarter     INTEGER,
                month       INTEGER,
                month_name  TEXT,
                day         INTEGER,
                day_of_week INTEGER,
                day_name    TEXT,
                is_weekend  INTEGER
            )
        """))
    return sqlite_engine


@pytest.fixture
def engine_with_salesperson(sqlite_engine):
    """SQLite avec table DimSalesPerson SCD2."""
    with sqlite_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE DimSalesPerson (
                salesperson_key  INTEGER PRIMARY KEY AUTOINCREMENT,
                salesperson_id   INTEGER,
                full_name        TEXT,
                job_title        TEXT,
                territory_id     REAL,
                start_valid_date TEXT,
                end_valid_date   TEXT,
                is_current       INTEGER
            )
        """))
    return sqlite_engine


# ── Tests load_static_dim ─────────────────────────────────────────────────────

def test_load_static_dim_insert_new(engine_with_territory):
    """Nouvelles lignes → insérées."""
    df = pd.DataFrame({
        "territory_id":   [1, 2],
        "territory_name": ["Northwest", "Northeast"],
        "country_code":   ["US", "US"],
        "continent":      ["North America", "North America"]
    })
    load_static_dim(df, "DimTerritory", "territory_id", engine_with_territory)

    with engine_with_territory.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimTerritory"), conn)
    assert len(result) == 2


def test_load_static_dim_no_duplicate(engine_with_territory):
    """Relancer le load → pas de doublons."""
    df = pd.DataFrame({
        "territory_id":   [1, 2],
        "territory_name": ["Northwest", "Northeast"],
        "country_code":   ["US", "US"],
        "continent":      ["North America", "North America"]
    })
    load_static_dim(df, "DimTerritory", "territory_id", engine_with_territory)
    load_static_dim(df, "DimTerritory", "territory_id", engine_with_territory)

    with engine_with_territory.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimTerritory"), conn)
    assert len(result) == 2  # toujours 2, pas 4


def test_load_static_dim_insert_only_new(engine_with_territory):
    """Seulement les nouveaux sont insérés."""
    df1 = pd.DataFrame({
        "territory_id":   [1],
        "territory_name": ["Northwest"],
        "country_code":   ["US"],
        "continent":      ["North America"]
    })
    df2 = pd.DataFrame({
        "territory_id":   [1, 2],
        "territory_name": ["Northwest", "Northeast"],
        "country_code":   ["US", "US"],
        "continent":      ["North America", "North America"]
    })
    load_static_dim(df1, "DimTerritory", "territory_id", engine_with_territory)
    load_static_dim(df2, "DimTerritory", "territory_id", engine_with_territory)

    with engine_with_territory.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimTerritory"), conn)
    assert len(result) == 2  # 1 existant + 1 nouveau


# ── Tests load_dim_date ───────────────────────────────────────────────────────

def test_load_dim_date_insert(engine_with_dimdate):
    """Nouvelles dates → insérées."""
    df = pd.DataFrame({
        "date_key":    [20240101, 20240102],
        "full_date":   ["2024-01-01", "2024-01-02"],
        "year":        [2024, 2024],
        "quarter":     [1, 1],
        "month":       [1, 1],
        "month_name":  ["January", "January"],
        "day":         [1, 2],
        "day_of_week": [1, 2],
        "day_name":    ["Monday", "Tuesday"],
        "is_weekend":  [0, 0]
    })
    load_dim_date(df, engine_with_dimdate)

    with engine_with_dimdate.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimDate"), conn)
    assert len(result) == 2


def test_load_dim_date_no_duplicate(engine_with_dimdate):
    """Relancer le load → pas de doublons."""
    df = pd.DataFrame({
        "date_key":    [20240101],
        "full_date":   ["2024-01-01"],
        "year":        [2024],
        "quarter":     [1],
        "month":       [1],
        "month_name":  ["January"],
        "day":         [1],
        "day_of_week": [1],
        "day_name":    ["Monday"],
        "is_weekend":  [0]
    })
    load_dim_date(df, engine_with_dimdate)
    load_dim_date(df, engine_with_dimdate)

    with engine_with_dimdate.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimDate"), conn)
    assert len(result) == 1  # toujours 1


# ── Tests load_scd2 ───────────────────────────────────────────────────────────

def test_scd2_premier_chargement(engine_with_salesperson):
    """Premier chargement → toutes les lignes insérées avec is_current=1."""
    df = pd.DataFrame({
        "salesperson_id": [274, 275],
        "full_name":      ["Stephen Jiang", "Michael Blythe"],
        "job_title":      ["Sales Manager", "Sales Representative"],
        "territory_id":   [None, 2.0]
    })
    load_scd2(df, "DimSalesPerson", "salesperson_id",
              ["full_name", "job_title", "territory_id"], engine_with_salesperson)

    with engine_with_salesperson.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimSalesPerson"), conn)
    assert len(result) == 2
    assert all(result["is_current"] == 1)
    assert all(result["end_valid_date"].isna())


def test_scd2_nouveau_enregistrement(engine_with_salesperson):
    """Nouvel enregistrement → inséré avec is_current=1."""
    df1 = pd.DataFrame({
        "salesperson_id": [274],
        "full_name":      ["Stephen Jiang"],
        "job_title":      ["Sales Manager"],
        "territory_id":   [None]
    })
    df2 = pd.DataFrame({
        "salesperson_id": [274, 275],
        "full_name":      ["Stephen Jiang", "Michael Blythe"],
        "job_title":      ["Sales Manager", "Sales Representative"],
        "territory_id":   [None, 2.0]
    })
    load_scd2(df1, "DimSalesPerson", "salesperson_id",
              ["full_name", "job_title", "territory_id"], engine_with_salesperson)
    load_scd2(df2, "DimSalesPerson", "salesperson_id",
              ["full_name", "job_title", "territory_id"], engine_with_salesperson)

    with engine_with_salesperson.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimSalesPerson"), conn)
    assert len(result) == 2
    assert result[result["salesperson_id"] == 275]["is_current"].values[0] == 1


def test_scd2_modification_cree_nouvelle_version(engine_with_salesperson):
    """Modification → ancienne version fermée + nouvelle insérée."""
    df1 = pd.DataFrame({
        "salesperson_id": [275],
        "full_name":      ["Michael Blythe"],
        "job_title":      ["Sales Representative"],
        "territory_id":   [2.0]
    })
    df2 = pd.DataFrame({
        "salesperson_id": [275],
        "full_name":      ["Michael Blythe"],
        "job_title":      ["Senior Sales Representative"],  # ← modifié
        "territory_id":   [2.0]
    })
    load_scd2(df1, "DimSalesPerson", "salesperson_id",
              ["full_name", "job_title", "territory_id"], engine_with_salesperson)
    load_scd2(df2, "DimSalesPerson", "salesperson_id",
              ["full_name", "job_title", "territory_id"], engine_with_salesperson)

    with engine_with_salesperson.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimSalesPerson WHERE salesperson_id = 275"), conn)

    assert len(result) == 2  # 2 versions
    ancienne = result[result["is_current"] == 0]
    nouvelle = result[result["is_current"] == 1]
    assert len(ancienne) == 1
    assert len(nouvelle) == 1
    assert ancienne["job_title"].values[0] == "Sales Representative"
    assert nouvelle["job_title"].values[0] == "Senior Sales Representative"
    assert ancienne["end_valid_date"].values[0] is not None


def test_scd2_inchange_pas_de_nouvelle_version(engine_with_salesperson):
    """Données inchangées → aucune nouvelle version créée."""
    df = pd.DataFrame({
        "salesperson_id": [275],
        "full_name":      ["Michael Blythe"],
        "job_title":      ["Sales Representative"],
        "territory_id":   [2.0]
    })
    load_scd2(df, "DimSalesPerson", "salesperson_id",
              ["full_name", "job_title", "territory_id"], engine_with_salesperson)
    load_scd2(df, "DimSalesPerson", "salesperson_id",
              ["full_name", "job_title", "territory_id"], engine_with_salesperson)

    with engine_with_salesperson.connect() as conn:
        result = pd.read_sql(text("SELECT * FROM DimSalesPerson"), conn)
    assert len(result) == 1  # toujours 1 version
    assert result["is_current"].values[0] == 1