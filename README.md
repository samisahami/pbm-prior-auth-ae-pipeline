# 🏥 PBM Prior Authorization Analytics Engineering Pipeline

End-to-end analytics engineering project modeling prior authorization workflows in a Pharmacy Benefit Management (PBM) context.

## 🔧 Tech Stack
- Databricks (Lakehouse + Dashboards)
- dbt (staging → marts)
- Delta / Iceberg tables
- SQL
- Git / GitHub

## 📊 Key Use Cases
- Prior authorization approval / denial rates
- Decision time SLA monitoring (Avg / Median / P90)
- Drug-level request volume analysis
- Operational bottleneck identification

## 🧬 dbt Models & Lineage
Detailed dbt models, tests, and lineage graphs live here:

👉 **[dbt project documentation](pbm_prior_auth_dbt/README.md)**

## 📈 Databricks Dashboard
Screenshots and walkthrough of the Databricks dashboard:

👉 *(add dashboard images here next)*

##   Highlights
- Bronze → staging → analytics-ready marts
- Fact / dimension modeling
- SLA-focused metrics
- Production-style lineage via `dbt docs`
