# Retail Inventory Intelligence Suite 🛒📊

![Python](https://img.shields.io/badge/Python-3.9-blue)
![SQL](https://img.shields.io/badge/PostgreSQL-18-orange)
![Type](https://img.shields.io/badge/Architecture-ELT-purple)
![Status](https://img.shields.io/badge/Status-Completed-green)

**An end-to-end data analytics platform engineered to solve the "Overstock vs. Stockout" dilemma in high-velocity retail supply chains.**

This project operationalizes raw logistics data into a prescriptive decision engine, integrating automated ELT pipelines, business-centric KPIs, and XGBoost predictive modeling. It is designed to shift supply chain operations from reactive firefighting to proactive profit optimization.

## Table of Contents
- [Project Overview](#project-overview)
- [Meet the Team](#meet-the-team)
- [Solution Overview](#solution-overview)
- [Features](#features)
- [Technical Execution & Decision Flow](#technical-execution--decision-flow)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contacts](#contacts)
- [License](#license)


## **Project Overview**
UrbanRetail Co., a high-velocity retail chain, is experiencing significant profit leakage due to a disconnect between logistics data and operational decision-making. Despite possessing rich sales and supply chain datasets, the lack of integrated predictive intelligence has created an "Inventory Paradox": simultaneous stockouts on high-margin items and capital trapped in dead stock. This forces store managers to rely on gut feeling rather than data, leading to reactive firefighting instead of proactive optimization.

### Key Problem Statements

* **Revenue Leakage:** Frequent stockouts due to a lack of integrated analytics system and underutilization of data.

* **Inflated Holding Costs:** Systemic overstocking is driving up storage costs and trapping working capital in slow-moving inventory.

* **Inventory Opacity:** Poor SKU visibility across the store network hinders effective stock transfers and balancing.

* **Reactive Operations:** Decision-making is retrospective rather than predictive, forcing managers to "firefight" issues after they occur.

* **Data Underutilization:** Rich sales and logistics datasets remain siloed and descriptive, failing to drive algorithmic optimization.

## Meet the Team
- **Avinash Choudhary** - **Lead Product Analyst & Engineer**
- **Abhishek Choudhary** - **Machine Learning Engineer**

## Solution Overview
This project builds an end-to-end *Prescriptive Inventory Decision Engine*, starting with a Python-based *ELT pipeline* that transforms raw OLTP transaction logs into a high-performance *Star Schema data warehouse (PostgreSQL)*. *Business-critical KPIs* such as Projected Stockout Loss, Critical Coverage Ratio, and Forecast WAPE are computed using SQL-based *OLAP views*. An external XGBoost forecast bias classifier is integrated to contextualize demand signals and flag unreliable predictions. 

All analytical intelligence is consolidated into an Action-First Dashboard for store managers, converting complex backend logic into immediate, actionable decisions that enable proactive management of overstock and stockout risk, low-inventory alerts, forecast reliability, and real-time inventory recommendations.

## Features
- **Automated ELT Pipeline:** Orchestrates the extraction, loading, and transformation of raw logs into a normalized Star Schema warehouse.
- **Modular Data Architecture:** Organized into clear layers (Raw $\to$ DWH $\to$ Reporting) with robust configuration management and execution logging.
- **Business-Centric KPIs:** Defined key KPIs, each mapped to business problems like Projected Stockout Loss and Forecast Reliability Score.
- **ML-Driven Forecast Audit:** Integrates an XGBoost classifier to detect forecast bias (Over/Under-estimation), flagging unreliable predictions.
- **Action-First Dashboard:**  Prescriptive, action-first dashboard enabling store managers to proactively manage stockouts, overstock, and replenishment.


## Technical Execution & Decision Flow

This project was designed as an **end-to-end decision intelligence system**, converting raw inventory and forecast data into actionable business recommendations.

### Step 1: Data Architecture (Star Schema)

A scalable **3-layer PostgreSQL architecture** was designed to clearly separate ingestion, storage, and analytics, ensuring both performance and maintainability.

#### **Architecture Layers:**

1. **Raw Layer (`raw`)**
   - Landing zone for unprocessed inventory snapshots and raw 7-day demand forecasts. Mirrors OLTP-style ERP data with no transformations applied.

2. **Data Warehouse Layer (`dwh`)**
   - Normalized **Star Schema** optimized for historical analysis and KPI computation.
   - **Dimensions** - `dim_product`, `dim_store` ,`dim_date`
   - **Fact Table**    - `fct_daily_inventory` (inventory position, sales, forecast, pricing)

- **Reporting Layer (`rpt`)**
  - Pre-aggregated **OLAP views** containing business logic. Acts as the single source of truth for dashboards and downstream analytics.
  - Examples - `inventory_health`, `forecast_deviation`
---

### Step 2: ELT Pipeline Orchestration

A Python-based orchestration layer (`main.py`) simulates a **production-grade ELT workflow**, ensuring repeatability and automation.

**Pipeline Flow:**

1. **Simulation (OLTP Emulation)**
   - `data_generator.py` generates synthetic daily transaction logs.
   - Mimics real-time ERP inventory and sales feeds.

2. **Extraction & Loading**
   - Raw CSV logs are ingested directly into the PostgreSQL `raw` schema.

3. **Transformation**
     - Clean and standardize raw data
     - Append historical records to the DWH
     - Refresh reporting-layer OLAP views

This design ensures that **dashboards always reflect the latest operational state**.

---

### Step 3: Strategic KPI Definition

To bridge the gap between **data visibility** and **decision-making**, five strategic KPIs were defined. Each KPI is mapped to a **specific business problem** and clears represent the **present and future risks** helping store managers to make better business decisions for inventory management.

---

| KPI                                        | Business Problem                                                 | Metric Definition                                                                          | Core Logic (Simplified)                                                                                                                                                                                                                                                                                                                  | Operational Action                                                                                            |
| ------------------------------------------ | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Projected Stockout Loss (Next 24h)**     | Immediate revenue lost if SKUs stock out before replenishment    | Revenue at risk from predicted inventory shortfall in next 24 hours                        | $\small \max(\text{Next Day forecast} - \text{On Hand Inventory},0)\times\text{Unit Price}$                                                                                                                                                                                                                                              | Prioritize emergency replenishment by **revenue impact**; expedite on-order deliveries.                       |
| **Low Inventory SKUs (DoS < 2)**           | Service-level risk for SKUs likely to stock out within lead time | % of SKUs with inventory coverage below a 2-day safety threshold                           | $\small DoS = \text{Full Days Covered} + \dfrac{\text{Remaining Inventory}}{\text{Demand of Next Partial Day}}$                                                                                                                                                                                                                          | Increase on-order quantities for SKUs with DoS < 2.                                                           |
| **Capital Locked in Overstock (DoS > 4)**  | Working capital tied in excess inventory, raising holding cost   | Monetary value of inventory beyond a 4-day efficient supply cap                            | $\small (\text{Total Inventory} - \text{Total 4 days demand}) \times \text{Unit Price}$                                                                                                                                                                                                                                                  | Target SKUs for markdowns, promotions, or redistribution.                                                     |
| **Inventory Velocity (Avg Life on Shelf)** | Slow movers create dead stock and clutter shelf space            | Avg days an SKU sits in inventory before sale                                              | <div align="center"><img src="https://latex.codecogs.com/svg.latex?\begin{gathered}\text{Turnover}=\frac{\sum_i(\text{Turnover}_i\times\text{UnitsSold}_{i,30})}{\sum_i\text{UnitsSold}_{i,30}}\\[2ex]\text{Avg Life on Shelf}=\frac{30}{\text{Turnover}}\end{gathered}" title="Turnover and Shelf Life Formulas" /></div>                                                                                                                                  | Fast movers → expand shelf space & shorten reorder cycles.<br>Slow movers → clearance, delist, or reallocate. |
| **Forecast Reliability Score (WAPE)**      | Low trust in forecasts causes manual overrides and stock risk    | Volume-weighted forecast error emphasizing high-selling SKUs and error direction/frequency | <div align="center"><img src="https://latex.codecogs.com/svg.latex?\begin{gathered}\text{Portfolio KPI}=\frac{\sum(\text{WAPE}_i\times\text{UnitsSold}_i)}{\sum\text{UnitsSold}_i}\\[2ex]\text{WAPE}_{\text{over}}=\frac{\sum_{F_t>A_t}(\text{WAPE}_i\times\text{UnitsSold}_i)}{\sum_{F_t>A_t}\text{UnitsSold}_i}\\[2ex]\text{Freq}_{\text{over}}=\frac{\sum\mathbf{1}(F_t>A_t)}{N}\end{gathered}" title="Portfolio and Forecast KPIs" /></div> | Adjust safety stock by measured risk; monitor over/under frequencies; fix model issues promptly.              |

**Outcome:**  
These KPIs convert raw forecasts and inventory positions into **prioritized actions**, enabling faster decisions with measurable financial impact.

### Step 4: ML Model
Trained Xgboost and SVC linear classfier model using grid search method to classify the demand forecasted bias, selected the best model based on maximum F1 score, then applied an F1-optimized probability threshold (instead of 0.5) to handle class imbalance and control the precision–recall tradeoff. The trained models and artifacts were saved using joblib for direct reuse in the ELT pipeline. Using the saved models, I predicted the next 7-day forecast accuracy flags (over-forecast, under-forecast, accurate) to identify bias in demand forecasts, and updated the ELT pipeline to retrain only monthly (at month-end) and refresh 7-day predictions everyday on dashboard/data, which feed downstream dashboard KPIs.

### Step 5: Action-First Dashboard

**Purpose:**

- An Action-First Inventory Intelligence dashboard that turns warehouse-grade analytics and model outputs into immediate, operational decisions—reducing stockouts, trimming holding costs, and improving SKU visibility across stores.

**Target Audience:**

- Store Managers (Execution): Need instant, daily "Reorder" vs. "Hold" signals to manage fast-moving stock.
- Supply Chain Analysts (Oversight): Need to monitor network-wide financial risk (Stockout Loss) and model performance.

**Key Capabilities & Features:**

- **Operational Alerts & Triage:** Automatic flagging of inventory status for each SKUs, including potential Stockouts, Overstock, and Critical Coverage (DoS < 2 Days).
- **Prescriptive Decision Engine:** Removes guesswork by mapping analytical status directly to business recommandations.
- **Holistic Inventory Visibility:** Provides a complete financial and operational snapshot, tracking Total Inventory, category-wise breakdowns, On-Hand vs. On-Order positions, and Working Capital trends over time to monitor liquidity.
- **Forecast Reliability Audit:** A transparency module designed to build trust in the model. It tracks WAPE (Forecast Deviation), analyzes the Frequency of Error Direction (Over-forecast vs. Under-forecast), and measures the magnitude of Error in each direction to determine the impact of the error.
- **Turnover Velocity Heatmap:** Solves "Poor SKU Visibility" by visualizing turnover rates for every product at every store ID. This instantly segregates "Fast Movers" from "Slow Movers" to optimize shelf assortment.
- **Multi-Dimensional Drilldown:** Enables a high-speed investigative workflow with a navigation path from Portfolio $\rightarrow$ Store $\rightarrow$ SKU. Dynamic filters for Date Range, Category, and Product allow users to isolate specific KPI health issues in seconds.

**Business Impact:**

- **Reduces Firefighting:** Shifts the store team from "reacting to empty shelves" to "predicting replenishment needs."
- **Secures Revenue:** Proactively closes the supply gap to prevent lost sales on high-margin items.
- **Capital Efficiency:** Identifies overstock immediately, allowing for faster markdowns to free up working capital.


## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/avinashchoudhary2004/Retail-Inventory-Intelligence-Suite.git

    cd Retail-Inventory-Intelligence-Suite
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Database Configuration:**
    * Create a local PostgreSQL database (e.g., `inventory_db`).
    * Update connection credentials in `scripts/config/db_config.py`.

4.  **Initialize Warehouse:**
    Execute the DDL scripts to build the Star Schema and seed historical data:
    * Run `SQL queries/01_warehouse_definition.sql`
    * Run `SQL queries/02_load_history.sql`

## Usage

**Run the Orchestrator:**
Execute `main.py` with a date argument to trigger the full daily workflow. This script automatically generates synthetic OLTP transactions (simulating live ERP traffic), executes the ELT pipeline, and refreshes the OLAP reporting views.

```bash
python main.py 
```

## **Project Structure**

```text
.
├── main.py                               # Pipeline Orchestrator (Entry Point)
├── dashboard/                            # Business Intelligence Layer
│   ├── Inventory Risk & Optimization.twbx    # Final Interactive Dashboard (Tableau File)
│   ├── Inventory Risk Dashboards.pdf         # Executive PDF Export
│   └── data/                                 # OLAP Reporting Tables
├── data/                                 # Data Lake / Storage Layer
│   ├── seed_historic_data/                   # Raw historic data
│   ├── archive/                              # Daily processed batch data
├── scripts/                              # Data Engineering Core
│   ├── config/db_config.py                   # Database connection management
│   ├── elt_pipeline.py                       # Core Extract-Load-Transform logic
│   └── data_generator.py                     # OLTP transaction data simulation module
├── SQL queries/                          # Transformation & Logic Layer
│   ├── 01_warehouse_defination.sql           # DDL: Warehouse setup (Raw, DWH, RPT)
│   ├── 02_load_history.sql                   # Initial historical data load
│   ├── 03_daily_batch_load.sql               # Incremental batch processing logic
│   └── 04_analytics_marts.sql                # DQL: KPI definitions & Reporting Views
├── ML model/                             # Machine Learning Integration
│   ├── model.zz/                             # Serialized XGBoost model objects
│   ├── Forecast Deviation.csv                # Model performance audit data
│   └── predictions_output.csv                # Raw forecast outputs for ingestion
├── logs/                                 # Pipeline execution monitoring logs
├── ERD_DWH.pdf                           # Entity Relationship Diagram
├── requirements.txt                      # Project dependencies
└── README.md                             # Documentation
```

## Contacts
Contributions are welcome! Please fork the repository and submit a pull request. For any questions or support, contact:
1. **Avinash Choudhary**: avinashchoudhary20040@gmail.com
2. **Abhishek Choudhary**: abhic.fin@gmail.com

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
