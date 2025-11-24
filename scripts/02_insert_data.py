# scripts/02_insert_data.py

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from config.catalog_config import get_catalog


def main():
    print("📥 Starting data ingestion...")

    catalog = get_catalog()
    table = catalog.load_table("customer_db.customer_master")

    print("📄 Table loaded:", table.identifier)

    # Read CSV
    df = pd.read_csv("data/sample_customer_master.csv")
    print("📊 CSV Loaded:")
    print(df.head())

    # Convert to Arrow
    arrow_table = pa.Table.from_pandas(df)

    # FORCE schema to match Iceberg (int32, string, string)
    arrow_table = arrow_table.set_column(
        0, "customer_id", pc.cast(arrow_table["customer_id"], pa.int32())
    )

    print("🔧 Schema after cast:")
    print(arrow_table.schema)

    print("📝 Appending to Iceberg...")
    table.append(arrow_table)

    print("✅ Data ingestion completed successfully!")


if __name__ == "__main__":
    main()
