# Electricity Data Collector

This repository is part of the **[Volatility Spillovers](https://github.com/franrolotti/volatility-spillovers)** project.
It provides an automated pipeline to download, update, and store electricity market data from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/).

The project uses **Apache Airflow** to orchestrate tasks and **Docker Compose** for local development.
Data is stored in **Parquet** format and organized for downstream processing with [dbt](https://github.com/franrolotti/volatility-spillovers-methodology) and visualization.

---

## Features

* Download electricity time series data from ENTSO-E.
* Store data incrementally:

  * If no local file exists → fetch from a user-defined start date up to the latest available.
  * If a file exists → fetch only missing dates and append to the existing dataset.
* Standardized storage in Parquet files.
* Daily scheduling with Airflow.

---

## Repository structure

```
electricity-data-collector/
├── dags/                # Airflow DAG definitions
├── edc/                 # ETL logic (ENTSO-E client, file handlers, etc.)
├── scripts/             # Helper scripts for testing/debugging
├── docker-compose.yml   # Local Docker setup
├── .env.example         # Example environment configuration
└── README.md
```

---

## Requirements

* [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
* ENTSO-E API key (you can [request one here](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html))

---

## Quickstart (local with Docker + Airflow)

1. **Clone the repository**

   ```bash
   git clone https://github.com/franrolotti/electricity-data-collector.git
   cd electricity-data-collector
   ```

2. **Set up environment variables**
   Copy the example file and edit it:

   ```bash
   cp .env.example .env
   ```

   Update with your values:

   ```
   ENTSOE_API_TOKEN=your_api_token_here
   EDC_TIMEZONE=Europe/Madrid   # or your local timezone
   ```

3. **Start Airflow + Postgres**

   ```bash
   docker compose up -d
   ```

   This will start:

   * `airflow-db`: PostgreSQL database for Airflow
   * `airflow`: Airflow scheduler + webserver

4. **Access the Airflow UI**
   Open [http://localhost:8080](http://localhost:8080) in your browser.

5. **Trigger a DAG**

   * Enable the `edc_collector` DAG in the UI.
   * This will download electricity data and write to `data/standard/`.

---

## Reading

To read the file in jupyter notebook after running the airflow task run:

```bash
notebooks/show_parket.ipynb
```

---

## Project links

* **Collector (this repo):** [electricity-data-collector](https://github.com/franrolotti/electricity-data-collector)
* **Methodology (dbt + spillover models):** [volatility-spillovers-methodology](https://github.com/franrolotti/volatility-spillovers-methodology)
* **Visualization (Streamlit/Dash):** [electricity-visualization](https://github.com/franrolotti/electricity-visualization)


