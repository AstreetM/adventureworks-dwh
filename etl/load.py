# etl/load.py
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from etl.utils import log


# ── Chargement statique (DimTerritory, DimShipMethod) ────────────────────────
def load_static_dim(df: pd.DataFrame, table: str, id_col: str, engine):
    """
    Chargement simple pour les dimensions statiques.
    INSERT uniquement les nouveaux enregistrements.
    """
    log(f"[LOAD] Chargement {table}...")

    with engine.begin() as conn:
        # Charger les IDs existants
        existing = pd.read_sql(
            f"SELECT {id_col} FROM {table}", conn
        )[id_col].tolist()

        # Filtrer uniquement les nouveaux
        new_rows = df[~df[id_col].isin(existing)]

        if len(new_rows) == 0:
            log(f"[LOAD] {table} — aucun nouvel enregistrement")
            return

        new_rows.to_sql(
            table, conn,
            if_exists="append",
            index=False,
            chunksize=500
        )
        log(f"[LOAD] {table} — {len(new_rows)} lignes insérées")


# ── Chargement SCD Type 2 ─────────────────────────────────────────────────────
def load_scd2(
    df_new: pd.DataFrame,
    table: str,
    id_col: str,
    business_cols: list,
    engine
):
    """
    Chargement SCD Type 2 :
    - Nouveaux   → INSERT is_current=1
    - Modifiés   → UPDATE ancienne version + INSERT nouvelle version
    - Inchangés  → rien
    """
    log(f"[LOAD] SCD2 {table}...")
    now = datetime.now()

    with engine.begin() as conn:
        # Lire uniquement les colonnes métier des lignes actives
        cols_to_read = [id_col] + business_cols
        df_db = pd.read_sql(
            f"SELECT {', '.join(cols_to_read)} "
            f"FROM {table} WHERE is_current = 1",
            conn
        )

        if len(df_db) == 0:
            # Première fois — tout insérer
            df_insert = df_new.copy()
            df_insert["start_valid_date"] = now
            df_insert["end_valid_date"]   = None
            df_insert["is_current"]       = 1
            df_insert.to_sql(
                table, conn,
                if_exists="append",
                index=False,
                chunksize=500
            )
            log(f"[LOAD] {table} — premier chargement : {len(df_insert)} lignes")
            return

        # Merger source vs DB sur la clé métier
        merged = df_new.merge(
            df_db, on=id_col,
            how="left",
            suffixes=("", "_db")
        )

        # ── Cas 1 : Nouveaux (absent de la DB)
        new_rows = merged[
            merged[[f"{c}_db" for c in business_cols][0]].isna()
        ].copy()

        # ── Cas 2 : Modifiés (présent mais au moins une colonne différente)
        def is_changed(row):
            for col in business_cols:
                val_new = row[col]
                val_db  = row[f"{col}_db"]
                if pd.isna(val_new) and pd.isna(val_db):
                    continue
                if val_new != val_db:
                    return True
            return False

        existing_rows = merged[merged[
            [f"{c}_db" for c in business_cols][0]
        ].notna()].copy()
        changed_rows = existing_rows[
            existing_rows.apply(is_changed, axis=1)
        ].copy()

        log(f"[LOAD] {table} — nouveaux: {len(new_rows)}, "
            f"modifiés: {len(changed_rows)}")

        # Fermer les anciennes versions des modifiés
        if len(changed_rows) > 0:
            changed_ids = tuple(changed_rows[id_col].tolist())
            if len(changed_ids) == 1:
                changed_ids = f"({changed_ids[0]})"
            conn.execute(text(f"""
                UPDATE {table}
                SET is_current = 0, end_valid_date = :now
                WHERE {id_col} IN {changed_ids}
                AND is_current = 1
            """), {"now": now})

        # Insérer nouveaux + nouvelles versions des modifiés
        to_insert = pd.concat([new_rows, changed_rows], ignore_index=True)
        if len(to_insert) > 0:
            # Garder uniquement les colonnes source (pas les colonnes _db)
            source_cols = [c for c in to_insert.columns
                          if not c.endswith("_db")]
            to_insert = to_insert[source_cols].copy()
            to_insert["start_valid_date"] = now
            to_insert["end_valid_date"]   = None
            to_insert["is_current"]       = 1
            to_insert.to_sql(
                table, conn,
                if_exists="append",
                index=False,
                chunksize=500
            )

        log(f"[LOAD] {table} — {len(to_insert)} lignes insérées")


# ── Chargement DimDate ────────────────────────────────────────────────────────
def load_dim_date(df: pd.DataFrame, engine):
    """
    Chargement incrémental de DimDate.
    INSERT uniquement les date_key absents.
    """
    log("[LOAD] Chargement DimDate...")

    with engine.begin() as conn:
        existing_keys = pd.read_sql(
            "SELECT date_key FROM DimDate", conn
        )["date_key"].tolist()

        new_rows = df[~df["date_key"].isin(existing_keys)]

        if len(new_rows) == 0:
            log("[LOAD] DimDate — aucune nouvelle date")
            return

        new_rows.to_sql(
            "DimDate", conn,
            if_exists="append",
            index=False,
            chunksize=500
        )
        log(f"[LOAD] DimDate — {len(new_rows)} nouvelles dates insérées")


# ── Chargement FactSalesHeader ────────────────────────────────────────────────
def load_fact_sales_header(df: pd.DataFrame, engine):
    """
    Chargement incrémental de FactSalesHeader.
    - Nouveaux sales_order_id → INSERT
    - Modifiés → UPDATE via table temporaire
    """
    log("[LOAD] Chargement FactSalesHeader...")

    with engine.begin() as conn:
        # Charger uniquement les clés + total_due existants
        existing = pd.read_sql(
            "SELECT sales_order_id, total_due FROM FactSalesHeader",
            conn
        )

        merged = df.merge(
            existing, on="sales_order_id",
            how="left",
            suffixes=("", "_db")
        )

        # Nouveaux
        new_rows = merged[merged["total_due_db"].isna()].copy()
        new_rows = new_rows[[c for c in new_rows.columns
                             if not c.endswith("_db")]]

        # Modifiés
        changed = merged[
            merged["total_due_db"].notna() &
            (merged["total_due"].round(2) != merged["total_due_db"].round(2))
        ].copy()
        changed = changed[[c for c in changed.columns
                           if not c.endswith("_db")]]

        # INSERT nouveaux
        if len(new_rows) > 0:
            new_rows.to_sql(
                "FactSalesHeader", conn,
                if_exists="append",
                index=False,
                chunksize=500
            )
            log(f"[LOAD] FactSalesHeader — {len(new_rows)} insérées")

        # UPDATE modifiés via table temporaire
        if len(changed) > 0:
            changed.to_sql(
                "##header_updates_tmp", conn,
                if_exists="replace",
                index=False
            )
            conn.execute(text("""
                UPDATE f
                SET f.subtotal          = t.subtotal,
                    f.tax_amount        = t.tax_amount,
                    f.freight           = t.freight,
                    f.total_due         = t.total_due,
                    f.days_to_ship      = t.days_to_ship,
                    f.customer_key      = t.customer_key,
                    f.territory_key     = t.territory_key,
                    f.salesperson_key   = t.salesperson_key,
                    f.shipmethod_key    = t.shipmethod_key
                FROM FactSalesHeader f
                INNER JOIN ##header_updates_tmp t
                ON f.sales_order_id = t.sales_order_id
            """))
            log(f"[LOAD] FactSalesHeader — {len(changed)} mises à jour")


# ── Chargement FactSales ──────────────────────────────────────────────────────
def load_fact_sales(df: pd.DataFrame, engine):
    """
    Chargement incrémental de FactSales.
    - Nouveaux sales_order_detail_id → INSERT
    - Modifiés → UPDATE via table temporaire
    """
    log("[LOAD] Chargement FactSales...")

    with engine.begin() as conn:
        # Charger uniquement les clés + line_total existants
        existing = pd.read_sql(
            "SELECT sales_order_detail_id, line_total FROM FactSales",
            conn
        )

        merged = df.merge(
            existing, on="sales_order_detail_id",
            how="left",
            suffixes=("", "_db")
        )

        # Nouveaux
        new_rows = merged[merged["line_total_db"].isna()].copy()
        new_rows = new_rows[[c for c in new_rows.columns
                             if not c.endswith("_db")]]

        # Modifiés
        changed = merged[
            merged["line_total_db"].notna() &
            (merged["line_total"].round(2) != merged["line_total_db"].round(2))
        ].copy()
        changed = changed[[c for c in changed.columns
                           if not c.endswith("_db")]]

        # INSERT nouveaux
        if len(new_rows) > 0:
            new_rows.to_sql(
                "FactSales", conn,
                if_exists="append",
                index=False,
                chunksize=500
            )
            log(f"[LOAD] FactSales — {len(new_rows)} insérées")

        # UPDATE modifiés via table temporaire
        if len(changed) > 0:
            changed.to_sql(
                "##sales_updates_tmp", conn,
                if_exists="replace",
                index=False
            )
            conn.execute(text("""
                UPDATE f
                SET f.order_qty             = t.order_qty,
                    f.unit_price            = t.unit_price,
                    f.unit_price_discount   = t.unit_price_discount,
                    f.line_total            = t.line_total,
                    f.standard_cost         = t.standard_cost,
                    f.margin                = t.margin,
                    f.product_key           = t.product_key,
                    f.customer_key          = t.customer_key,
                    f.territory_key         = t.territory_key,
                    f.salesperson_key       = t.salesperson_key,
                    f.shipmethod_key        = t.shipmethod_key
                FROM FactSales f
                INNER JOIN ##sales_updates_tmp t
                ON f.sales_order_detail_id = t.sales_order_detail_id
            """))
            log(f"[LOAD] FactSales — {len(changed)} mises à jour")


# ── Point d'entrée principal ──────────────────────────────────────────────────
def load_all(transformed: dict, dwh_engine):
    """
    Charge toutes les tables dans AdventureWorksDWH
    dans le bon ordre (dimensions avant faits).
    """
    # 1 — Dimensions statiques
    load_static_dim(
        transformed["dim_territory"],
        "DimTerritory", "territory_id", dwh_engine
    )
    load_static_dim(
        transformed["dim_shipmethod"],
        "DimShipMethod", "shipmethod_id", dwh_engine
    )

    # 2 — DimDate
    load_dim_date(transformed["dim_date"], dwh_engine)

    # 3 — Dimensions SCD2
    load_scd2(
        transformed["dim_salesperson"],
        "DimSalesPerson", "salesperson_id",
        ["full_name", "job_title", "territory_id"],
        dwh_engine
    )
    load_scd2(
        transformed["dim_product"],
        "DimProduct", "product_id",
        ["product_name", "list_price", "standard_cost",
         "category", "subcategory", "color"],
        dwh_engine
    )
    load_scd2(
        transformed["dim_customer"],
        "DimCustomer", "customer_id",
        ["full_name", "customer_type", "city", "state", "country"],
        dwh_engine
    )

    # 4 — Faits (après toutes les dimensions)
    load_fact_sales_header(transformed["fact_sales_header"], dwh_engine)
    load_fact_sales(transformed["fact_sales"], dwh_engine)

    log("[LOAD] ✅ Chargement complet terminé !")