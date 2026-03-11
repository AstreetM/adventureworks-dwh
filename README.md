# AdventureWorks DWH

An end-to-end ETL pipeline that transforms the AdventureWorks2019 OLTP database (70+ tables) into a star schema data warehouse, orchestrated with Apache Airflow.

---

## Business Context

Adventure Works Cycles is a bicycle manufacturer and distributor. This data warehouse was built to answer 5 key business axes:

| Axis | Questions answered |
|---|---|
| **Sales & Revenue** | What are total sales by period? What are the margins? |
| **Products** | Which categories/subcategories perform best? |
| **Customers** | Who are our top customers? Individual vs Store? |
| **Geography & Territory** | Which territories generate the most revenue? |
| **Delivery & Operations** | What is the average delivery time? Online vs in-store? |

---

## Architecture

```
AdventureWorks2019 (OLTP, 70+ tables)
        │
        │  7 SQL Views (extraction layer)
        ▼
   ETL Pipeline (Python)
   ┌─────────────┐
   │  extract.py │  → reads from 7 views
   │ validate.py │  → null checks, dedup, quarantine
   │transform.py │  → builds star schema + surrogate keys
   │    load.py  │  → SCD2, incremental loads
   └─────────────┘
        │
        ▼
AdventureWorksDWH (Star Schema)
        │
        ▼
   Apache Airflow (orchestration, daily at 6:00 AM)
```

---

## Star Schema

**2 Fact Tables:**

| Table | Grain | Rows |
|---|---|---|
| `FactSales` | 1 row per order line | 121,317 |
| `FactSalesHeader` | 1 row per order | 31,465 |

**6 Dimension Tables:**

| Table | Type | Rows |
|---|---|---|
| `DimProduct` | SCD Type 2 | 266 |
| `DimCustomer` | SCD Type 2 | 19,820 |
| `DimSalesPerson` | SCD Type 2 | 17 |
| `DimTerritory` | Static | 10 |
| `DimShipMethod` | Static | 5 |
| `DimDate` | Generated | 1,139 |

---

## Project Structure

```
adventureworks-dwh/
├── config.py                  ← DB connections (not committed)
├── config.example.py          ← connection template
├── main.py                    ← pipeline entry point
├── explore.py                 ← data profiling script (run once)
├── etl/
│   ├── extract.py             ← reads 7 source views
│   ├── validate.py            ← data quality + quarantine
│   ├── transform.py           ← star schema + surrogate key lookups
│   ├── load.py                ← SCD2, incremental loads
│   └── utils.py               ← engine factory + logging
├── schema/
│   └── create_tables.sql      ← DWH DDL
├── dags/
│   └── etl_adventureworks_dag.py  ← Airflow DAG (4 tasks)
├── tests/
│   ├── test_validate.py       ← 11 tests
│   ├── test_transform.py      ← 14 tests
│   └── test_load.py           ← 9 tests
└── rejected/                  ← quarantined rows (CSV)
```

---

## ETL Pipeline

### Extract
Reads 7 SQL Server views from AdventureWorks2019:
`v_DimTerritory`, `v_DimShipMethod`, `v_DimSalesPerson`, `v_DimProduct`, `v_DimCustomer`, `v_FactSalesHeader`, `v_FactSales`

### Validate
- Required column checks (raises error if missing)
- Date conversion: `order_date`, `ship_date`, `due_date` → `datetime`
- Deduplication on business keys
- Quarantine: rows with NULL business keys or impossible values → `rejected/` CSV
- Acceptable NULLs kept: `salesperson_id` (online orders), `city/state/country` (customers without address)

### Transform
- `DimDate` generated in Python (no source table)
- `date_to_key()` converts dates to `YYYYMMDD` integer format
- Surrogate key lookups via pandas merge after dimensions are loaded
- `fillna` applied: `color/size → "N/A"`, `weight → 0`, `city/state/country → "Unknown"`

### Load

| Strategy | Applied to |
|---|---|
| Static insert (new only) | DimTerritory, DimShipMethod |
| SCD Type 2 | DimProduct, DimCustomer, DimSalesPerson |
| Incremental insert + temp table UPDATE | FactSalesHeader, FactSales |
| Date key dedup | DimDate |

**SCD Type 2 logic:**
```
New record  → INSERT (is_current=1, end_valid_date=NULL)
Modified    → UPDATE old (is_current=0) + INSERT new version
Unchanged   → nothing
```

---

## Airflow DAG

**DAG ID:** `etl_adventureworks`  
**Schedule:** daily at 06:00 UTC  
**Tasks:** `extract → validate → load_dims → load_facts`

---

## Setup

### Prerequisites
- SQL Server with AdventureWorks2019 installed
- Python 3.11+
- ODBC Driver 17 for SQL Server
- Apache Airflow 2.x (Docker + WSL2)

### Installation

```bash
git clone https://github.com/AstreetM/adventureworks-dwh.git
cd adventureworks-dwh
pip install pandas sqlalchemy pyodbc pytest
```

### Configuration

```bash
cp config.example.py config.py
# Edit config.py with your SQL Server credentials
```

### Database setup (SSMS)

```sql
-- 1. Create extraction views in AdventureWorks2019
-- Run: schema/create_views.sql

-- 2. Create DWH tables in AdventureWorksDWH
-- Run: schema/create_tables.sql
```

### Run pipeline

```bash
python main.py
```

### Run tests

```bash
pytest tests/ -v
# Expected: 34/34 passed
```

---

## Data Quality Notes

| Column | Table | Situation | Decision |
|---|---|---|---|
| `salesperson_id` | FactSales, FactSalesHeader | NULL for online orders | Keep — business logic |
| `city/state/country` | DimCustomer | NULL for 48% of Individual customers | Keep → fillna "Unknown" |
| `territory_id` | DimSalesPerson | NULL for managers | Keep — business logic |
| `margin` | FactSales | 29,161 negative rows | Keep + log — valid business data |

---

## Stack

| Tool | Usage |
|---|---|
| Python 3.11 | ETL logic |
| pandas | data transformation |
| SQLAlchemy | DB connections |
| pyodbc | SQL Server driver |
| SQL Server 2019 | source + destination |
| Apache Airflow 2.x | orchestration |
| Docker / WSL2 | Airflow runtime |
| pytest | unit testing (34 tests) |

---

## Author

**Astri M.** — Data Engineering Portfolio Project
