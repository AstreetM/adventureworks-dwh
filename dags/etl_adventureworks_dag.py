# dags/etl_adventureworks_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajouter le chemin du projet pour les imports
sys.path.insert(0, "/opt/airflow/adventureworks-dwh")

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

# ── Paramètres par défaut ─────────────────────────────────────────────────────
default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Définition du DAG ─────────────────────────────────────────────────────────
with DAG(
    dag_id="etl_adventureworks",
    description="ETL AdventureWorks2019 → AdventureWorksDWH",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * *",   # Tous les jours à 6h00
    catchup=False,
    tags=["adventureworks", "etl", "dwh"],
) as dag:

    # ── Helpers ───────────────────────────────────────────────────────────────
    def get_engines():
        return get_engine(SOURCE_DB), get_engine(DWH_DB)

    def read_dim(table, cols, dwh_engine):
        with dwh_engine.connect() as conn:
            return pd.read_sql(
                text(f"SELECT {', '.join(cols)} FROM {table} WHERE is_current = 1"),
                conn
            )

    def read_dim_static(table, cols, dwh_engine):
        with dwh_engine.connect() as conn:
            return pd.read_sql(
                text(f"SELECT {', '.join(cols)} FROM {table}"),
                conn
            )

    # ── TASK 1 : Extract ──────────────────────────────────────────────────────
    def task_extract(**context):
        source_engine, _ = get_engines()
        data = extract_all(source_engine)
        log("[DAG] Extract terminé")
        for name, df in data.items():
            log(f"  {name:<20} → {len(df):>7} lignes")
        # Sérialiser pour XCom
        context["ti"].xcom_push(
            key="data",
            value={name: df.to_json() for name, df in data.items()}
        )

    # ── TASK 2 : Validate ─────────────────────────────────────────────────────
    def task_validate(**context):
        raw = context["ti"].xcom_pull(key="data", task_ids="extract")
        data = {name: pd.read_json(js) for name, js in raw.items()}

        # Reconvertir les dates après désérialisation JSON
        for df_name in ["fact_sales", "fact_sales_header"]:
            for col in ["order_date", "ship_date", "due_date"]:
                if col in data[df_name].columns:
                    data[df_name][col] = pd.to_datetime(
                        data[df_name][col], unit="ms"
                    )

        validated = validate_all(data)
        log("[DAG] Validation terminée")
        context["ti"].xcom_push(
            key="data",
            value={name: df.to_json() for name, df in validated.items()}
        )

    # ── TASK 3 : Load dimensions ──────────────────────────────────────────────
    def task_load_dims(**context):
        raw = context["ti"].xcom_pull(key="data", task_ids="validate")
        data = {name: pd.read_json(js) for name, js in raw.items()}

        # Reconvertir les dates
        for df_name in ["fact_sales", "fact_sales_header"]:
            for col in ["order_date", "ship_date", "due_date"]:
                if col in data[df_name].columns:
                    data[df_name][col] = pd.to_datetime(
                        data[df_name][col], unit="ms"
                    )

        _, dwh_engine = get_engines()

        # DimDate
        dim_date = build_dim_date(data)
        load_dim_date(dim_date, dwh_engine)

        # Statiques
        load_static_dim(build_dim_territory(data),  "DimTerritory",  "territory_id",  dwh_engine)
        load_static_dim(build_dim_shipmethod(data),  "DimShipMethod", "shipmethod_id", dwh_engine)

        # SCD2
        load_scd2(build_dim_salesperson(data), "DimSalesPerson", "salesperson_id",
                  ["full_name", "job_title", "territory_id"], dwh_engine)
        load_scd2(build_dim_product(data), "DimProduct", "product_id",
                  ["product_name", "list_price", "standard_cost",
                   "category", "subcategory", "color"], dwh_engine)
        load_scd2(build_dim_customer(data), "DimCustomer", "customer_id",
                  ["full_name", "customer_type", "city", "state", "country"], dwh_engine)

        log("[DAG] Chargement dimensions terminé")

    # ── TASK 4 : Load faits ───────────────────────────────────────────────────
    def task_load_facts(**context):
        raw = context["ti"].xcom_pull(key="data", task_ids="validate")
        data = {name: pd.read_json(js) for name, js in raw.items()}

        # Reconvertir les dates
        for df_name in ["fact_sales", "fact_sales_header"]:
            for col in ["order_date", "ship_date", "due_date"]:
                if col in data[df_name].columns:
                    data[df_name][col] = pd.to_datetime(
                        data[df_name][col], unit="ms"
                    )

        _, dwh_engine = get_engines()

        # Relire les dims depuis le DWH
        dim_customer_db    = read_dim("DimCustomer",    ["customer_key",    "customer_id"],    dwh_engine)
        dim_product_db     = read_dim("DimProduct",     ["product_key",     "product_id"],     dwh_engine)
        dim_salesperson_db = read_dim("DimSalesPerson", ["salesperson_key", "salesperson_id"], dwh_engine)
        dim_territory_db   = read_dim_static("DimTerritory",  ["territory_key",  "territory_id"],  dwh_engine)
        dim_shipmethod_db  = read_dim_static("DimShipMethod", ["shipmethod_key", "shipmethod_id"], dwh_engine)

        # FactSalesHeader
        fact_header = build_fact_sales_header(
            data, dim_customer_db, dim_territory_db,
            dim_salesperson_db, dim_shipmethod_db
        )
        load_fact_sales_header(fact_header, dwh_engine)

        # FactSales
        fact_sales = build_fact_sales(
            data, dim_product_db, dim_customer_db,
            dim_territory_db, dim_salesperson_db, dim_shipmethod_db
        )
        load_fact_sales(fact_sales, dwh_engine)

        log("[DAG] Chargement faits terminé")

    # ── Définition des tâches ─────────────────────────────────────────────────
    t_extract  = PythonOperator(task_id="extract",    python_callable=task_extract,    provide_context=True)
    t_validate = PythonOperator(task_id="validate",   python_callable=task_validate,   provide_context=True)
    t_dims     = PythonOperator(task_id="load_dims",  python_callable=task_load_dims,  provide_context=True)
    t_facts    = PythonOperator(task_id="load_facts", python_callable=task_load_facts, provide_context=True)

    # ── Ordre d'exécution ─────────────────────────────────────────────────────
    t_extract >> t_validate >> t_dims >> t_facts