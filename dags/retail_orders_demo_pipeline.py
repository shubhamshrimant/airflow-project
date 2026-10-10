from datetime import datetime

import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="retail_orders_pipeline",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["youtube", "data-engineering", "python"],
)
def retail_orders_pipeline():

    @task
    def extract_orders():
        orders = [
            {
                "order_id": 1001,
                "customer_id": "C001",
                "product": "Laptop",
                "quantity": 1,
                "unit_price": 75000,
            },
            {
                "order_id": 1002,
                "customer_id": "C002",
                "product": "Mouse",
                "quantity": 2,
                "unit_price": 1200,
            },
            {
                "order_id": 1003,
                "customer_id": "C003",
                "product": "Keyboard",
                "quantity": 1,
                "unit_price": 2500,
            },
        ]

        print(f"Extracted {len(orders)} orders")

        return orders

    @task
    def transform_orders(orders):
        transformed_orders = []

        for order in orders:
            order_value = order["quantity"] * order["unit_price"]

            transformed_orders.append(
                {
                    **order,
                    "order_value": order_value,
                }
            )

        print("Transformation completed")

        return transformed_orders

    @task
    def load_orders(orders):
        print("Loading orders...")

        for order in orders:
            print(
                f"Order {order['order_id']} "
                f"has value {order['order_value']}"
            )

        print(f"Loaded {len(orders)} orders")

    orders = extract_orders()

    transformed_orders = transform_orders(orders)

    load_orders(transformed_orders)


retail_orders_pipeline()