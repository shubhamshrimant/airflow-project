"""Daily retail ETL demonstrating intervals, branching and dynamic task mapping.

Airflow 3.3.x | Existing Docker Compose + PostgreSQL business database.
Connection ID: retail_postgres
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

POSTGRES_CONN_ID = "retail_postgres"
COUNTRY_DATA_DIR = Path("/opt/airflow/dags/data/countries")
COUNTRIES = ["IN", "US", "UK"]


@dag(
    dag_id="retail_scheduled_dynamic_pipeline",
    description="Daily retail ETL with data intervals, branching and dynamic mapping",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    schedule="0 9 * * *",  # Every day at 09:00 Asia/Kolkata
    catchup=True,
    max_active_runs=1,
    tags=["youtube", "scheduling", "dynamic-mapping", "branching"],
)
def retail_scheduled_dynamic_pipeline():

    @task
    def show_data_interval() -> dict:
        """Show the logical processing window for this DAG run."""
        context = get_current_context()
        start = context["data_interval_start"]
        end = context["data_interval_end"]
        logical_date = context["logical_date"]
        print(f"Logical date: {logical_date}")
        print(f"Data interval start (inclusive): {start}")
        print(f"Data interval end (exclusive): {end}")
        print("This daily run represents the previous day's data window.")
        return {
            "interval_start": start.to_iso8601_string(),
            "interval_end": end.to_iso8601_string(),
        }

    @task.branch
    def choose_processing_path() -> str:
        """Route weekend runs differently from weekday runs."""
        context = get_current_context()
        interval_start = context["data_interval_start"]
        # pendulum: Monday=0 ... Sunday=6
        if interval_start.day_of_week in (5, 6):
            print(f"{interval_start} is a weekend interval")
            return "weekend_processing"
        print(f"{interval_start} is a weekday interval")
        return "weekday_processing"

    @task
    def weekday_processing() -> str:
        print("Running standard weekday processing")
        return "weekday"

    @task
    def weekend_processing() -> str:
        print("Running weekend processing / reduced-volume path")
        return "weekend"

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def continue_after_branch() -> None:
        print("The selected branch completed; continuing the shared workflow")

    @task
    def get_countries() -> list[str]:
        """Return a small list; Airflow creates one mapped task per country."""
        print(f"Countries to process: {COUNTRIES}")
        return COUNTRIES

    @task
    def create_country_orders_table() -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS country_orders (
                processing_date DATE NOT NULL,
                country_code VARCHAR(2) NOT NULL,
                order_id INTEGER NOT NULL,
                customer_id VARCHAR(20) NOT NULL,
                order_date DATE NOT NULL,
                product TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (processing_date, country_code, order_id)
            );
            """
        )

    @task
    def load_country(country_code: str, interval: dict) -> dict:
        """Load one country's file. Composite key makes reruns idempotent."""
        file_path = COUNTRY_DATA_DIR / f"{country_code.lower()}_orders.csv"
        if not file_path.exists():
            raise FileNotFoundError(f"Missing country file: {file_path}")

        processing_date = datetime.fromisoformat(
            interval["interval_start"].replace("Z", "+00:00")
        ).date()

        records = []
        with file_path.open("r", newline="", encoding="utf-8") as file:
            for row in csv.DictReader(file):
                records.append((
                    processing_date,
                    country_code,
                    int(row["order_id"]),
                    row["customer_id"],
                    row["order_date"],
                    row["product"],
                    int(row["quantity"]),
                    float(row["unit_price"]),
                ))

        if not records:
            raise ValueError(f"No records found for country {country_code}")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO country_orders (
                        processing_date, country_code, order_id, customer_id,
                        order_date, product, quantity, unit_price
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (processing_date, country_code, order_id)
                    DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        order_date = EXCLUDED.order_date,
                        product = EXCLUDED.product,
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

        print(f"{country_code}: upserted {len(records)} rows for {processing_date}")
        return {"country": country_code, "rows": len(records)}

    @task
    def summarize_country_loads(results: list[dict]) -> None:
        total = sum(item["rows"] for item in results)
        print(f"Mapped country tasks completed: {len(results)}")
        print(f"Total rows submitted across countries: {total}")
        for item in results:
            print(f"{item['country']}: {item['rows']} rows")

    interval = show_data_interval()
    branch = choose_processing_path()
    weekday = weekday_processing()
    weekend = weekend_processing()
    branch >> [weekday, weekend]
    joined = continue_after_branch()
    [weekday, weekend] >> joined

    countries = get_countries()
    create_table = create_country_orders_table()
    mapped_loads = load_country.partial(interval=interval).expand(country_code=countries)
    summary = summarize_country_loads(mapped_loads)

    joined >> [create_table, countries]
    [create_table, countries] >> mapped_loads
    summary


retail_scheduled_dynamic_pipeline()
