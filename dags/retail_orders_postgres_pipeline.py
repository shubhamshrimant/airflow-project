"""Retail orders ETL: CSV -> PostgreSQL staging -> SQL analytics."""

from __future__ import annotations

import csv
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


CSV_PATH = Path("/opt/airflow/dags/data/orders.csv")
POSTGRES_CONN_ID = "retail_postgres"


@dag(
    dag_id="retail_orders_postgres_pipeline",
    description="Load retail orders from CSV into PostgreSQL and transform with SQL",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["youtube", "postgresql", "etl"],
)
def retail_orders_postgres_pipeline():

    @task
    def create_staging_table() -> None:
        """Create the staging table if it does not already exist."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS staging_orders (
                order_id INTEGER PRIMARY KEY,
                customer_id VARCHAR(20) NOT NULL,
                order_date DATE NOT NULL,
                product TEXT NOT NULL,
                category TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

    @task
    def extract_and_load_csv() -> int:
        """Read CSV and upsert records into PostgreSQL staging."""
        if not CSV_PATH.exists():
            raise FileNotFoundError(
                f"CSV not found at {CSV_PATH}. Copy data/orders.csv "
                "to the mounted Airflow dags/data/ directory."
            )

        records = []
        with CSV_PATH.open("r", newline="", encoding="utf-8") as file:
            for row in csv.DictReader(file):
                records.append(
                    (
                        int(row["order_id"]),
                        row["customer_id"],
                        row["order_date"],
                        row["product"],
                        row["category"],
                        int(row["quantity"]),
                        float(row["unit_price"]),
                    )
                )

        if not records:
            raise ValueError("Input CSV contains no order records.")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO staging_orders (
                        order_id, customer_id, order_date, product,
                        category, quantity, unit_price
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        order_date = EXCLUDED.order_date,
                        product = EXCLUDED.product,
                        category = EXCLUDED.category,
                        quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price,
                        loaded_at = NOW();
                    """,
                    records,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(f"Upserted {len(records)} records into staging_orders")
        return len(records)

    @task
    def transform_staging_to_analytics() -> None:
        """Rebuild the analytics table using SQL."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql_file = Path("/opt/airflow/dags/sql/transform_orders.sql")
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        hook.run(sql_file.read_text(encoding="utf-8"))

    @task
    def report_row_count() -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        count = hook.get_first("SELECT COUNT(*) FROM analytics_orders;")[0]
        revenue = hook.get_first(
            "SELECT COALESCE(SUM(order_value), 0) FROM analytics_orders;"
        )[0]
        print(f"Analytics rows: {count}; total revenue: {revenue}")

    create = create_staging_table()
    loaded = extract_and_load_csv()
    transformed = transform_staging_to_analytics()
    report = report_row_count()

    create >> loaded >> transformed >> report


retail_orders_postgres_pipeline()
