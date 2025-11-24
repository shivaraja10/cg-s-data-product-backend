# scripts/01_create_catalog.py

from config.catalog_config import get_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType
from pyiceberg.expressions import NestedField


def main():
    print("🔍 Starting catalog creation...")

    catalog = get_catalog()
    print("📁 Catalog loaded successfully.")

    # Create namespace
    try:
        catalog.create_namespace("customer_db")
        print("✅ Namespace 'customer_db' created.")
    except Exception as e:
        print(f"ℹ️ Namespace already exists or error: {e}")

    # Define schema
    print("📝 Creating schema...")
    schema = Schema(
        NestedField(id=1, name="customer_id", type=IntegerType(), required=False),
        NestedField(id=2, name="customer_name", type=StringType(), required=False),
        NestedField(id=3, name="region", type=StringType(), required=False),
    )

    # Create table
    print("📦 Creating table customer_master...")
    try:
        table = catalog.create_table(
            identifier="customer_db.customer_master",
            schema=schema,
            location="file:./warehouse/customer_master",
            properties={"format-version": "2"}
        )
        print("✅ Table created successfully.")
        print(table)
    except Exception as e:
        print(f"⚠️ Table creation error: {e}")


if __name__ == "__main__":
    main()
