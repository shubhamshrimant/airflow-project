# Airflow project architecture

## Docker Compose services

```mermaid
flowchart TB
    Browser["Browser"]
    PgAdminUI["pgAdmin UI<br/>localhost:5050"]
    HostDB["Host database clients"]

    subgraph Compose["Docker Compose network"]
        API["Airflow API server<br/>airflow-apiserver:8080"]
        Scheduler["Airflow scheduler"]
        Processor["Airflow DAG processor"]
        Worker["Airflow Celery worker"]
        Triggerer["Airflow triggerer"]
        Init["Airflow init"]
        Redis[("Redis<br/>Celery broker")]

        Metadata[("PostgreSQL: airflow<br/>Airflow metadata DB")]
        Retail[("PostgreSQL: retail<br/>Business data DB")]
        PgAdmin["pgAdmin"]

        Scheduler -->|queue tasks| Redis
        Redis -->|deliver tasks| Worker

        API --> Metadata
        Scheduler --> Metadata
        Processor --> Metadata
        Worker --> Metadata
        Triggerer --> Metadata
        Init --> Metadata

        Worker -->|retail_postgres connection| Retail
        PgAdmin --> Metadata
        PgAdmin --> Retail
    end

    Browser -->|Airflow UI, localhost:8080| API
    Browser --> PgAdminUI
    PgAdminUI --> PgAdmin
    HostDB -->|localhost:5432| Metadata
    HostDB -->|localhost:5433| Retail
```

Airflow metadata and retail business data are stored in separate PostgreSQL
services and Docker volumes. Redis is the Celery message broker; it is not a
database for either Airflow metadata or retail orders. The Airflow services
share the project DAG, logs, config, and plugins mounts.

## Retail orders DAG

```mermaid
flowchart LR
    CSV["dags/data/orders.csv<br/>mounted at /opt/airflow/dags/data/orders.csv"]
    Create["create_staging_table<br/>Python task creates table"]
    Load["extract_and_load_csv<br/>CSV read and upsert"]
    Transform["transform_staging_to_analytics<br/>runs dags/sql/transform_orders.sql"]
    Report["report_row_count<br/>count and revenue"]

    subgraph RetailDB["Retail PostgreSQL database: retail"]
        Staging[("staging_orders")]
        Analytics[("analytics_orders")]
    end

    CSV --> Load
    Create -. task dependency .-> Load
    Create -. creates table .-> Staging
    Load -->|upsert rows| Staging
    Staging --> Transform
    Transform --> Analytics
    Analytics --> Report
```

The task order is create staging table, load/upsert CSV rows, rebuild analytics,
then report row count and revenue. Airflow tasks connect to the business
database using the Airflow connection ID `retail_postgres` and Docker hostname
`retail-postgres`.

`dags/sql/create_analytics_table.sql` is present in the project, but the DAG
currently creates `analytics_orders` through `transform_orders.sql` instead.
