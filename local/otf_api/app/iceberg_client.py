# app/iceberg_client.py
import os
from pyiceberg.catalog.sql import SqlCatalog

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CATALOG_DB = os.path.join(BASE_DIR, "catalog.db")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")

# Ensure paths exist
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

catalog = SqlCatalog(
    name="local",
    uri=f"sqlite:///{CATALOG_DB}",
    warehouse=WAREHOUSE_DIR
)