import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 3306)
DB_NAME = os.getenv("DB_NAME")

# Create database connection
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Load Excel
excel_path = os.path.join("assets", "Rechnungen_2025.xlsx")
df = pd.read_excel(excel_path)

# Normalize orders and items
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
        "order_date": pd.to_datetime(row["Order_Date"], errors='coerce').date()
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

# Create DataFrames
df_orders = pd.DataFrame(orders).drop_duplicates(subset=["bestellnummer"])
df_items = pd.DataFrame(items)

# Upload orders
df_orders.to_sql("excel_orders", engine, if_exists="append", index=False)

# Map order_id using bestellnummer
with engine.connect() as conn:
    order_map = pd.read_sql("SELECT id, bestellnummer FROM excel_orders", conn)

df_items = df_items.merge(order_map, on="bestellnummer")
df_items.rename(columns={"id": "order_id"}, inplace=True)
df_items.drop(columns=["bestellnummer"], inplace=True)

# Upload order_items
df_items.to_sql("excel_order_items", engine, if_exists="append", index=False)

print("ETL completed successfully.")