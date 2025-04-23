import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

from sqlalchemy import text

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)  # Default PostgreSQL port
DB_NAME = os.getenv("DB_NAME")

# Create database connection

# MySQL connection string
# engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# PostgreSQL connection string
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS excel_etl"))

asset_files = [
    "Rechnungen_2020.xlsx",
    "Rechnungen_2021.xlsx",
    "Rechnungen_2022.xlsx",
    "Rechnungen_2023.xlsx",
    "Rechnungen_2025.xlsx"
]

for filename in asset_files:
    year = filename.split("_")[1].split(".")[0]
    excel_path = os.path.join("assets", filename)
    df = pd.read_excel(excel_path)

    orders = []
    items = []

    for _, row in df.iterrows():
        order = {
            "bestellnummer": row["Bestellnummer"],
            "rechnungsnummer": row["Rechnungsnummer"],
            "zahlungsreferenznummer": row["Zahlungsreferenznummer"],
            "rechnungsadresse": row["Rechnungsadresse"],
            "lieferadresse": row["Lieferadresse"],
            "zahlbetrag": row["Zahlbetrag"],
            "country_code": row["Country_Code"],
            "order_date": pd.to_datetime(row["Order_Date"], dayfirst=True, errors='coerce').date()
        }
        orders.append(order)

        for i in range(1, 6):  # up to 5 articles
            artikel = row.get(f"Artikelname {i}")
            asin = row.get(f"ASIN {i}")
            qty = row.get(f"Quantity {i}")
            if pd.notna(artikel) and pd.notna(asin):
                items.append({
                    "bestellnummer": row["Bestellnummer"],
                    "artikelname": artikel,
                    "asin": asin,
                    "quantity": int(qty) if pd.notna(qty) else 1
                })

    df_orders = pd.DataFrame(orders).drop_duplicates().dropna(subset=["bestellnummer"])
    df_items = pd.DataFrame(items)

    schema_name = "excel_etl"
    orders_table_name = f"excel_{year}_orders"
    items_table_name = f"excel_{year}_items"

    # Ensure orders table exists with 'id SERIAL PRIMARY KEY'
    create_orders_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{orders_table_name} (
        id SERIAL PRIMARY KEY,
        bestellnummer TEXT,
        rechnungsnummer TEXT,
        zahlungsreferenznummer TEXT,
        rechnungsadresse TEXT,
        lieferadresse TEXT,
        zahlbetrag NUMERIC,
        country_code TEXT,
        order_date DATE
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_orders_table_sql))

    # Deduplicate orders: avoid inserting orders that already exist
    with engine.connect() as conn:
        existing_orders = pd.read_sql(f"SELECT bestellnummer FROM {schema_name}.{orders_table_name}", conn)
    df_orders = df_orders[~df_orders["bestellnummer"].isin(existing_orders["bestellnummer"])]

    # Insert orders only if not empty
    if not df_orders.empty:
        with engine.begin() as connection:
            df_orders.to_sql(orders_table_name, con=connection, schema=schema_name, if_exists="append", index=False)

    # Read order IDs
    with engine.connect() as conn:
        order_map = pd.read_sql(f"SELECT id, bestellnummer FROM {schema_name}.{orders_table_name}", conn)

    df_items = df_items.merge(order_map, on="bestellnummer")
    df_items.rename(columns={"id": "order_id"}, inplace=True)
    df_items.drop(columns=["bestellnummer"], inplace=True)

    # Deduplicate items: avoid inserting items that already exist (order_id + asin)
    if not df_items.empty:
        try:
            with engine.connect() as conn:
                existing_items = pd.read_sql(f"SELECT order_id, asin FROM {schema_name}.{items_table_name}", conn)
            df_items = df_items.merge(existing_items, on=["order_id", "asin"], how="left", indicator=True)
            df_items = df_items[df_items["_merge"] == "left_only"].drop(columns=["_merge"])
        except Exception as e:
            print(f"Items table {items_table_name} not found yet. Proceeding to insert all rows.")

    # Insert items only if not empty
    if not df_items.empty:
        with engine.begin() as connection:
            df_items.to_sql(items_table_name, con=connection, schema=schema_name, if_exists="append", index=False)

    print(f"Year {year}: ETL completed successfully.")