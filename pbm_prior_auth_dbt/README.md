# dbt Project (PBM Prior Auth)

This folder contains the dbt project used to build the analytics-ready models for the PBM Prior Authorization pipeline.

👉 **Main project overview (start here):**  
../../README.md

---

## Lineage Graphs

### fct_requests
![fct_requests lineage](docs/lineage/lineage_fct_requests.png)

### fct_events
![fct_events lineage](docs/lineage/lineage_fct_events.png)

---

## How to View Lineage Locally

```bash
dbt docs generate
dbt docs serve --port 8089





