"""Create the demo `retail` warehouse and load data in a chosen scenario.

    python -m scripts.seed_data --scenario clean     # everything passes
    python -m scripts.seed_data --scenario dirty     # duplicates, nulls, orphans, bad values
    python -m scripts.seed_data --scenario stale     # data stopped arriving (freshness breach)
    python -m scripts.seed_data --scenario drift     # upstream added/changed columns (schema break)
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import create_engine, text

DDL = """
DROP SCHEMA IF EXISTS retail CASCADE;
CREATE SCHEMA retail;
-- raw layer: deliberately NO constraints, so bad data can land and be caught by the DQ platform
CREATE TABLE retail.customers   (customer_id INT, email TEXT, full_name TEXT, country TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);
CREATE TABLE retail.products    (product_id INT, sku TEXT, name TEXT, category TEXT, price NUMERIC(12,2), updated_at TIMESTAMPTZ);
CREATE TABLE retail.orders      (order_id INT, customer_id INT, status TEXT, order_total NUMERIC(12,2), order_ts TIMESTAMPTZ, updated_at TIMESTAMPTZ);
CREATE TABLE retail.order_items (item_id INT, order_id INT, product_id INT, quantity INT, unit_price NUMERIC(12,2));
"""
COUNTRIES = ["IN", "US", "GB", "DE", "SG", "AU", "CA"]
CATEGORIES = ["books", "electronics", "home", "toys", "sports"]
STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled", "refunded"]


def build(n_customers, n_products, n_orders, rng, now):
    recent = lambda hours: now - timedelta(hours=rng.uniform(0.05, hours))
    customers = pd.DataFrame({
        "customer_id": range(1, n_customers + 1),
        "email": [f"user{i}@example.com" for i in range(1, n_customers + 1)],
        "full_name": [f"Customer {i}" for i in range(1, n_customers + 1)],
        "country": [rng.choice(COUNTRIES) for _ in range(n_customers)],
        "created_at": [now - timedelta(days=rng.randint(30, 700)) for _ in range(n_customers)],
        "updated_at": [recent(40) for _ in range(n_customers)],
    })
    products = pd.DataFrame({
        "product_id": range(1, n_products + 1),
        "sku": [f"SKU-{i:05d}" for i in range(1, n_products + 1)],
        "name": [f"Product {i}" for i in range(1, n_products + 1)],
        "category": [rng.choice(CATEGORIES) for _ in range(n_products)],
        "price": [round(rng.uniform(3, 900), 2) for _ in range(n_products)],
        "updated_at": [recent(60) for _ in range(n_products)],
    })
    orders = pd.DataFrame({
        "order_id": range(1, n_orders + 1),
        "customer_id": [rng.randint(1, n_customers) for _ in range(n_orders)],
        "status": [rng.choice(STATUSES) for _ in range(n_orders)],
        "order_total": [0.0] * n_orders,
        "order_ts": [recent(24 * 20) for _ in range(n_orders)],
        "updated_at": [recent(20) for _ in range(n_orders)],
    })
    rows, item_id = [], 1
    for oid in orders.order_id:
        for pid in rng.sample(range(1, n_products + 1), rng.randint(1, 4)):
            price = float(products.loc[pid - 1, "price"])
            rows.append((item_id, oid, pid, rng.randint(1, 5), price))
            item_id += 1
    items = pd.DataFrame(rows, columns=["item_id", "order_id", "product_id", "quantity", "unit_price"])
    totals = (items.quantity * items.unit_price).groupby(items.order_id).sum().round(2)
    orders["order_total"] = orders.order_id.map(totals)
    return customers, products, orders, items


def inject_dirty(customers, products, orders, items, rng):
    # uniqueness: duplicate customers (same id + same email)
    dup = customers.sample(8, random_state=1)
    customers = pd.concat([customers, dup], ignore_index=True)
    # completeness: missing emails / names / countries
    customers.loc[customers.sample(6, random_state=2).index, "email"] = None
    customers.loc[customers.sample(12, random_state=3).index, "full_name"] = None
    customers.loc[customers.sample(30, random_state=4).index, "country"] = None
    # validity: malformed emails
    customers.loc[customers.sample(10, random_state=5).index, "email"] = "not-an-email"
    # validity: bad price, null product name, duplicate sku
    products.loc[products.sample(3, random_state=6).index, "price"] = -5.0
    products.loc[products.sample(4, random_state=7).index, "name"] = None
    products.loc[1, "sku"] = products.loc[0, "sku"]
    # referential integrity: orders for customers that do not exist, items for missing orders/products
    orders.loc[orders.sample(25, random_state=8).index, "customer_id"] = 999_000 + rng.randint(1, 50)
    items.loc[items.sample(30, random_state=9).index, "order_id"] = 888_000 + rng.randint(1, 50)
    items.loc[items.sample(15, random_state=10).index, "product_id"] = 777_000
    # validity: unknown status, negative total, zero quantity
    orders.loc[orders.sample(12, random_state=11).index, "status"] = "teleported"
    orders.loc[orders.sample(5, random_state=12).index, "order_total"] = -10.0
    items.loc[items.sample(9, random_state=13).index, "quantity"] = 0
    return customers, products, orders, items


def make_stale(customers, products, orders, items, now):
    old = now - timedelta(days=6)
    orders["updated_at"] = old - pd.to_timedelta(range(len(orders)), unit="m")
    products["updated_at"] = old - pd.to_timedelta(range(len(products)), unit="m")
    return customers, products, orders, items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=["clean", "dirty", "stale", "drift"], default="dirty")
    ap.add_argument("--customers", type=int, default=500)
    ap.add_argument("--products", type=int, default=80)
    ap.add_argument("--orders", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    url = os.getenv("DQ_WAREHOUSE_URL", "postgresql+psycopg2://warehouse:warehouse@localhost:5432/warehouse")
    rng, now = random.Random(args.seed), datetime.now(timezone.utc)
    frames = build(args.customers, args.products, args.orders, rng, now)
    if args.scenario == "dirty":
        frames = inject_dirty(*frames, rng)
    elif args.scenario == "stale":
        frames = make_stale(*frames, now)

    engine = create_engine(url)
    with engine.begin() as conn:
        conn.exec_driver_sql(DDL)
    names = ["customers", "products", "orders", "order_items"]
    for name, df in zip(names, frames):
        df.to_sql(name, engine, schema="retail", if_exists="append", index=False, method="multi", chunksize=2000)
    if args.scenario == "drift":
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE retail.customers ADD COLUMN loyalty_tier TEXT"))      # new column
            conn.execute(text("ALTER TABLE retail.orders ALTER COLUMN order_total TYPE TEXT USING order_total::text"))  # type change
    print(f"Seeded scenario={args.scenario}: " + ", ".join(f"{n}={len(d)}" for n, d in zip(names, frames)))


if __name__ == "__main__":
    main()
