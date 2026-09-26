import argparse
import os
import random
from decimal import Decimal
from uuid import uuid4

import psycopg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=1000)
    args = parser.parse_args()

    if args.orders < 1:
        parser.error("--orders must be positive")

    rng = random.Random(42)
    run_id = uuid4().hex
    customers = []
    products = []

    with psycopg.connect(
        host="127.0.0.1",
        port=5433,
        dbname="commerce",
        user="commerce",
        password=os.environ["COMMERCE_DB_PASSWORD"],
    ) as conn:
        with conn.cursor() as cur:
            for i in range(100):
                cur.execute(
                    """
                    INSERT INTO commerce.customers
                        (full_name, email, city)
                    VALUES (%s, %s, %s)
                    RETURNING customer_id
                    """,
                    (
                        f"Client Test {i + 1}",
                        f"{run_id}.{i}@example.com",
                        rng.choice(["Casablanca", "Rabat", "Essaouira"]),
                    ),
                )
                customers.append(cur.fetchone()[0])

            for i in range(50):
                price = Decimal(rng.randint(1000, 100000)) / 100
                cur.execute(
                    """
                    INSERT INTO commerce.products
                        (sku, product_name, category, price)
                    VALUES (%s, %s, %s, %s)
                    RETURNING product_id
                    """,
                    (
                        f"GEN-{run_id}-{i}",
                        f"Produit Test {i + 1}",
                        rng.choice(["Informatique", "Maison", "Sport"]),
                        price,
                    ),
                )
                product_id = cur.fetchone()[0]
                products.append((product_id, price))

                cur.execute(
                    """
                    INSERT INTO commerce.inventory (product_id, quantity)
                    VALUES (%s, %s)
                    """,
                    (product_id, args.orders * 3),
                )

            for i in range(args.orders):
                selected = rng.sample(products, rng.randint(1, 4))
                items = [
                    (product_id, price, rng.randint(1, 3))
                    for product_id, price in selected
                ]
                total = sum(
                    (price * quantity for _, price, quantity in items),
                    Decimal("0.00"),
                )

                cur.execute(
                    """
                    INSERT INTO commerce.orders
                        (customer_id, status, amount)
                    VALUES (%s, 'CREATED', %s)
                    RETURNING order_id
                    """,
                    (rng.choice(customers), total),
                )
                order_id = cur.fetchone()[0]

                for product_id, price, quantity in items:
                    cur.execute(
                        """
                        INSERT INTO commerce.order_items
                            (order_id, product_id, quantity, unit_price)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (order_id, product_id, quantity, price),
                    )
                    cur.execute(
                        """
                        UPDATE commerce.inventory
                        SET quantity = quantity - %s
                        WHERE product_id = %s AND quantity >= %s
                        """,
                        (quantity, product_id, quantity),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("Insufficient stock")

                cur.execute(
                    """
                    INSERT INTO commerce.payments
                        (order_id, payment_reference, amount, method, status)
                    VALUES (%s, %s, %s, %s, 'SUCCESS')
                    """,
                    (
                        order_id,
                        f"GEN-{run_id}-{i}",
                        total,
                        rng.choice(["CARD", "TRANSFER"]),
                    ),
                )
                cur.execute(
                    """
                    UPDATE commerce.orders
                    SET status = 'PAID'
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )

    print(f"SUCCESS: 100 customers, 50 products, {args.orders} orders committed.")
    print(f"Run ID: {run_id}")


if __name__ == "__main__":
    main()
