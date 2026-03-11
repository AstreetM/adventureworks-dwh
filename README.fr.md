# AdventureWorks DWH

Pipeline ETL complet qui transforme la base OLTP AdventureWorks2019 (70+ tables) en entrepôt de données en schéma en étoile, orchestré avec Apache Airflow.

---

## Contexte métier

Adventure Works Cycles est un fabricant et distributeur de vélos. Cet entrepôt de données a été conçu pour répondre à 5 axes métier :

| Axe | Questions auxquelles on répond |
|---|---|
| **Ventes & Revenus** | Quels sont les revenus par période ? Quelles sont les marges ? |
| **Produits** | Quelles catégories/sous-catégories performent le mieux ? |
| **Clients** | Qui sont nos meilleurs clients ? Particulier vs Magasin ? |
| **Géographie & Territoire** | Quels territoires génèrent le plus de revenus ? |
| **Livraison & Opérations** | Quel est le délai moyen de livraison ? En ligne vs physique ? |

---

## Architecture

```
AdventureWorks2019 (OLTP, 70+ tables)
        │
        │  7 vues SQL (couche d'extraction)
        ▼
   Pipeline ETL (Python)
   ┌─────────────┐
   │  extract.py │  → lecture des 7 vues
   │ validate.py │  → contrôle qualité + quarantaine
   │transform.py │  → construction schéma en étoile + clés de substitution
   │    load.py  │  → SCD2, chargements incrémentiels
   └─────────────┘
        │
        ▼
AdventureWorksDWH (Schéma en étoile)
        │
        ▼
   Apache Airflow (orchestration, tous les jours à 6h00)
```

---

## Schéma en étoile

**2 tables de faits :**

| Table | Grain | Lignes |
|---|---|---|
| `FactSales` | 1 ligne par ligne de commande | 121 317 |
| `FactSalesHeader` | 1 ligne par commande | 31 465 |

**6 tables de dimensions :**

| Table | Type | Lignes |
|---|---|---|
| `DimProduct` | SCD Type 2 | 266 |
| `DimCustomer` | SCD Type 2 | 19 820 |
| `DimSalesPerson` | SCD Type 2 | 17 |
| `DimTerritory` | Statique | 10 |
| `DimShipMethod` | Statique | 5 |
| `DimDate` | Générée en Python | 1 139 |

---

## Structure du projet

```
adventureworks-dwh/
├── config.py                  ← connexions DB (non commité)
├── config.example.py          ← modèle de configuration
├── main.py                    ← point d'entrée du pipeline
├── explore.py                 ← script d'exploration (à lancer une fois)
├── etl/
│   ├── extract.py             ← lecture des 7 vues source
│   ├── validate.py            ← qualité des données + quarantaine
│   ├── transform.py           ← schéma en étoile + lookups clés de substitution
│   ├── load.py                ← SCD2, chargements incrémentiels
│   └── utils.py               ← factory engine + logging
├── schema/
│   └── create_tables.sql      ← DDL du DWH
├── dags/
│   └── etl_adventureworks_dag.py  ← DAG Airflow (4 tâches)
├── tests/
│   ├── test_validate.py       ← 11 tests
│   ├── test_transform.py      ← 14 tests
│   └── test_load.py           ← 9 tests
└── rejected/                  ← lignes rejetées en quarantaine (CSV)
```

---

## Pipeline ETL

### Extraction
Lecture de 7 vues SQL Server créées dans AdventureWorks2019 :
`v_DimTerritory`, `v_DimShipMethod`, `v_DimSalesPerson`, `v_DimProduct`, `v_DimCustomer`, `v_FactSalesHeader`, `v_FactSales`

### Validation
- Contrôle des colonnes obligatoires (erreur si manquante)
- Conversion des dates : `order_date`, `ship_date`, `due_date` → `datetime`
- Dédoublonnage sur les clés métier
- Quarantaine : lignes avec clé métier NULL ou valeurs impossibles → CSV dans `rejected/`
- NULL acceptables conservés : `salesperson_id` (commandes en ligne), `city/state/country` (clients sans adresse)

### Transformation
- `DimDate` générée entièrement en Python (pas de table source)
- `date_to_key()` convertit les dates au format entier `YYYYMMDD`
- Lookup des clés de substitution via merge pandas après chargement des dimensions
- `fillna` appliqués : `color/size → "N/A"`, `weight → 0`, `city/state/country → "Unknown"`

### Chargement

| Stratégie | Appliquée à |
|---|---|
| INSERT statique (nouveaux seulement) | DimTerritory, DimShipMethod |
| SCD Type 2 | DimProduct, DimCustomer, DimSalesPerson |
| INSERT incrémentiel + UPDATE via table temporaire | FactSalesHeader, FactSales |
| Dédup sur date_key | DimDate |

**Logique SCD Type 2 :**
```
Nouveau         → INSERT (is_current=1, end_valid_date=NULL)
Modifié         → UPDATE ancienne version (is_current=0) + INSERT nouvelle version
Inchangé        → rien
```

---

## DAG Airflow

**DAG ID :** `etl_adventureworks`
**Planification :** tous les jours à 06h00 UTC
**Tâches :** `extract → validate → load_dims → load_facts`

Les données transitent entre les tâches via XCom (sérialisation JSON).

---

## Installation

### Prérequis
- SQL Server avec AdventureWorks2019 installé
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
# Modifier config.py avec vos identifiants SQL Server
```

### Mise en place des bases (SSMS)

```sql
-- 1. Créer les vues dans AdventureWorks2019
-- Run: schema/create_views.sql

-- 2. Créer les tables dans AdventureWorksDWH
-- Run: schema/create_tables.sql
```

### Lancer le pipeline

```bash
python main.py
```

### Lancer les tests

```bash
pytest tests/ -v
# Résultat attendu : 34/34 passed
```

---

## Qualité des données

| Colonne | Table | Situation | Décision |
|---|---|---|---|
| `salesperson_id` | FactSales, FactSalesHeader | NULL pour commandes en ligne | Conservé — logique métier |
| `city/state/country` | DimCustomer | NULL pour 48% des clients | Conservé → fillna "Unknown" |
| `territory_id` | DimSalesPerson | NULL pour les managers | Conservé — logique métier |
| `margin` | FactSales | 29 161 lignes négatives | Conservé + loggé — donnée métier valide |

---

## Stack technique

| Outil | Usage |
|---|---|
| Python 3.11 | Logique ETL |
| pandas | Transformation des données |
| SQLAlchemy | Connexions base de données |
| pyodbc | Driver SQL Server |
| SQL Server 2019 | Source + destination |
| Apache Airflow 2.x | Orchestration |
| Docker / WSL2 | Environnement Airflow |
| pytest | Tests unitaires (34 tests) |

---

## Auteure

**Astri M.** — Projet portfolio Data Engineering
