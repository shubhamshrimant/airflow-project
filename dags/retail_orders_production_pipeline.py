"""Video 5: reliability, validation, retries and idempotent PostgreSQL loads."""

from __future__ import annotations

import csv
import logging
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/dags/data")
CSV_PATH = DATA_DIR / "orders.csv"
POSTGRES_CONN_ID = "retail_postgres"

REQUIRED_COLUMNS = {
    "order_id", "customer_id", "order_date", "product",
    "category", "quantity", "unit_price",
}


def read_and_validate_orders(csv_path: Path) -> list[dict]:
    """Read CSV and fail the task with actionable errors for invalid data."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {csv_path}")

    records: list[dict] = []
    seen_order_ids: set[int] = set()
    errors: list[str] = []

    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        actual_columns = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - actual_columns
        if missing:
            raise ValueError(f"CSV is missing required columns: {sorted(missing)}")

        for line_number, row in enumerate(reader, start=2):
            try:
                order_id = int(row["order_id"])
                quantity = int(row["quantity"])
                unit_price = Decimal(row["unit_price"])
                order_date = pendulum.parse(row["order_date"]).date()

                if order_id <= 0:
                    raise ValueError("order_id must be positive")
                if quantity <= 0:
                    raise ValueError("quantity must be greater than zero")
                if unit_price < 0:
                    raise ValueError("unit_price cannot be negative")
                if not row["customer_id"].strip():
                    raise ValueError("customer_id cannot be empty")
                if not row["product"].strip() or not row["category"].strip():
                    raise ValueError("product and category cannot be empty")
                if order_id in seen_order_ids:
                    raise ValueError(f"duplicate order_id: {order_id}")

                seen_order_ids.add(order_id)
                records.append({
                    "order_id": order_id,
                    "customer_id": row["customer_id"].strip(),
                    "order_date": order_date,
                    "product": row["product"].strip(),
                    "category": row["category"].strip(),
                    "quantity": quantity,
                    "unit_price": unit_price,
                })
            except (ValueError, TypeError, InvalidOperation) as exc:
                errors.append(f"Line {line_number}: {exc}")

    if errors:
        preview = "; ".join(errors[:5])
        raise ValueError(
            f"CSV validation failed with {len(errors)} error(s). "
            f"First errors: {preview}"
        )
    if not records:
        raise ValueError("CSV contains a header but no data records.")

    return records


@dag(
    dag_id="retail_orders_production_pipeline",
    description="Reliable CSV-to-PostgreSQL pipeline with validation and idempotent upserts",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=3),
    },
    tags=["youtube", "postgresql", "reliability", "idempotency"],
)
def retail_orders_production_pipeline():

    @task(retries=0)
    def validate_input() -> dict:
        """Validate source data and return only small metadata through XCom."""
        records = read_and_validate_orders(CSV_PATH)
        result = {
            "source_file": CSV_PATH.name,
            "row_count": len(records),
            "distinct_order_ids": len({r["order_id"] for r in records}),
        }
        logger.info("Validation successful: %s", result)
        return result

    @task(retries=2, retry_delay=timedelta(seconds=20))
    def demonstrate_transient_failure() -> str:
        """Fail the first attempt on purpose, then succeed on retry."""
        from airflow.sdk import get_current_context

        context = get_current_context()
        task_instance = context["ti"]
        if task_instance.try_number == 1:
            logger.warning("Intentional demo failure on first attempt.")
            raise RuntimeError("Simulated temporary upstream service failure")
        logger.info("Retry attempt succeeded.")
        return "Recovered after retry"

    @task
    def create_staging_table() -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run("""
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
        """)

    @task
    def load_orders(validation_result: dict) -> dict:
        """Upsert validated records atomically; safe to retry or rerun."""
        records = read_and_validate_orders(CSV_PATH)
        if len(records) != validation_result["row_count"]:
            raise ValueError(
                "Input file changed between validation and loading. "
                "Refusing to load a different record count."
            )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        try:
            with connection.cursor() as cursor:
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
                        loaded_at = NOW()
                    WHERE (
                        staging_orders.customer_id,
                        staging_orders.order_date,
                        staging_orders.product,
                        staging_orders.category,
                        staging_orders.quantity,
                        staging_orders.unit_price
                    ) IS DISTINCT FROM (
                        EXCLUDED.customer_id,
                        EXCLUDED.order_date,
                        EXCLUDED.product,
                        EXCLUDED.category,
                        EXCLUDED.quantity,
                        EXCLUDED.unit_price
                    );
                    """,
                    [
                        (
                            r["order_id"], r["customer_id"], r["order_date"],
                            r["product"], r["category"], r["quantity"],
                            r["unit_price"],
                        )
                        for r in records
                    ],
                )
            connection.commit()
        except Exception:
            connection.rollback()
            logger.exception("Database load failed; transaction rolled back.")
            raise
        finally:
            connection.close()

        result = {
            "source_rows": len(records),
            "distinct_order_ids": validation_result["distinct_order_ids"],
            "target_table": "staging_orders",
        }
        logger.info("Load completed: %s", result)
        return result

    @task
    def transform_analytics(load_result: dict) -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql_path = Path("/opt/airflow/dags/sql/transform_orders.sql")
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        hook.run(sql_path.read_text(encoding="utf-8"))
        logger.info(
            "Analytics table rebuilt after loading %s source rows.",
            load_result["source_rows"],
        )

    @task
    def verify_result(load_result: dict) -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        count = hook.get_first("SELECT COUNT(*) FROM staging_orders;")[0]
        analytics_count = hook.get_first(
            "SELECT COUNT(*) FROM analytics_orders;"
        )[0]
        logger.info(
            "Verification: source rows=%s, staging rows=%s, analytics rows=%s",
            load_result["source_rows"], count, analytics_count,
        )
        if count < load_result["distinct_order_ids"]:
            raise ValueError(
                "Target row count is lower than the distinct source order count."
            )
        if analytics_count != count:
            raise ValueError(
                "Analytics and staging row counts do not match."
            )

    validation = validate_input()
    retry_demo = demonstrate_transient_failure()
    create = create_staging_table()
    loaded = load_orders(validation)
    transformed = transform_analytics(loaded)
    verified = verify_result(loaded)

    validation >> retry_demo >> create >> loaded >> transformed >> verified


retail_orders_production_pipeline()
