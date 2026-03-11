# etl/validate.py
import numpy as np
import pandas as pd
from datetime import datetime
from etl.utils import log
import os

# Colonnes obligatoires par DataFrame
REQUIRED_COLS = {
    "dim_territory":     ["territory_id", "territory_name", "country_code", "continent"],
    "dim_shipmethod":    ["shipmethod_id", "ship_method_name"],
    "dim_salesperson":   ["salesperson_id", "full_name", "job_title"],
    "dim_product":       ["product_id", "product_name", "category", "subcategory"],
    "dim_customer":      ["customer_id", "full_name", "customer_type"],
    "fact_sales_header": ["sales_order_id", "customer_id", "order_date", "total_due"],
    "fact_sales":        ["sales_order_detail_id", "sales_order_id", "product_id",
                          "customer_id", "order_qty", "unit_price", "line_total"],
}

# Clés métier par DataFrame (quarantaine si NULL)
BUSINESS_KEYS = {
    "dim_territory":     ["territory_id"],
    "dim_shipmethod":    ["shipmethod_id"],
    "dim_salesperson":   ["salesperson_id"],
    "dim_product":       ["product_id"],
    "dim_customer":      ["customer_id"],
    "fact_sales_header": ["sales_order_id", "customer_id", "order_date"],
    "fact_sales":        ["sales_order_detail_id", "sales_order_id",
                          "product_id", "customer_id"],
}

# Clés de dédoublonnage par DataFrame
DEDUP_KEYS = {
    "dim_territory":     ["territory_id"],
    "dim_shipmethod":    ["shipmethod_id"],
    "dim_salesperson":   ["salesperson_id"],
    "dim_product":       ["product_id"],
    "dim_customer":      ["customer_id"],
    "fact_sales_header": ["sales_order_id"],
    "fact_sales":        ["sales_order_detail_id"],
}


def validate_and_clean(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Validation complète d'un DataFrame source.
    Retourne le DataFrame nettoyé.
    """
    log(f"[VALIDATE] Validation de {name} — {len(df)} lignes en entrée")
    report = {"input": len(df), "corrections": [], "rejected": 0}

    # ── ÉTAPE 1 : Contrôle colonnes obligatoires ─────────────────────────────
    required = REQUIRED_COLS.get(name, [])
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[VALIDATE] {name} — Colonnes manquantes : {missing}")

    # ── ÉTAPE 2 : Auto-corrections ────────────────────────────────────────────
    # Normaliser noms de colonnes
    df.columns = df.columns.str.strip().str.lower()

    # Remplacer chaînes vides par NaN
    df = df.replace("", np.nan)

    # Nettoyer colonnes numériques si elles contiennent des symboles
    for col in ["total_due", "subtotal", "unit_price", "line_total",
                "standard_cost", "list_price", "freight"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r"[$,€]", "", regex=True),
                errors="coerce"
            )


    # Convertir dates (type object → datetime)
    for col in ["order_date", "ship_date", "due_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # NULL acceptables (pas de quarantaine)
    # - salesperson_id : NULL pour commandes en ligne → NORMAL
    # - city/state/country dans dim_customer → NORMAL
    # - territory_id dans dim_salesperson → NORMAL

    # Valeurs aberrantes à logger mais pas rejeter
    if name == "fact_sales" and "margin" in df.columns:
        neg_margin = (df["margin"] < 0).sum()
        if neg_margin > 0:
            log(f"[VALIDATE] {name} — {neg_margin} lignes avec marge négative (conservées)")

    # ── ÉTAPE 3 : Dédoublonnage sur clé métier ────────────────────────────────
    dedup_keys = DEDUP_KEYS.get(name, [])
    before     = len(df)
    df         = df.drop_duplicates(subset=dedup_keys)
    dupes      = before - len(df)
    if dupes > 0:
        log(f"[VALIDATE] {name} — {dupes} doublons supprimés sur {dedup_keys}")

    # ── ÉTAPE 4 : Quarantaine ─────────────────────────────────────────────────
    biz_keys = BUSINESS_KEYS.get(name, [])
    if biz_keys:
        mask_null = df[biz_keys].isnull().any(axis=1)

        # Cas spécifiques : valeurs impossibles
        mask_neg = pd.Series(False, index=df.index)
        if "line_total" in df.columns:
            mask_neg = mask_neg | (df["line_total"] < 0)
        if "order_qty" in df.columns:
            mask_neg = mask_neg | (df["order_qty"] <= 0)
        if "total_due" in df.columns:
            mask_neg = mask_neg | (df["total_due"] < 0)

        mask_rejected = mask_null | mask_neg
        rejected      = df[mask_rejected]

        if len(rejected) > 0:
            os.makedirs("rejected", exist_ok=True)
            ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = f"rejected/{name}_rejected_{ts}.csv"
            rejected.to_csv(path, index=False)
            report["rejected"] = len(rejected)
            log(f"[VALIDATE] {name} — {len(rejected)} lignes rejetées → {path}")

        df = df[~mask_rejected]

    # ── Rapport final ─────────────────────────────────────────────────────────
    log(f"[VALIDATE] {name} — {len(df)} lignes acceptées / "
        f"{report['rejected']} rejetées")

    return df


def validate_all(data: dict) -> dict:
    """Valide tous les DataFrames du dictionnaire."""
    return {name: validate_and_clean(name, df) for name, df in data.items()}