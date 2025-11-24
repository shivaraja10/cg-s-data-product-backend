from config.catalog_config import get_catalog
import pyarrow as pa

def main():
    catalog = get_catalog()
    table = catalog.load_table("customer_db.customer_master")

    new_rows = [
        {
            "customer_id": 1004,
            "customer_name": "Delta Mart",
            "region": "West",
            "customer_segment": "Modern Trade"
        },
        {
            "customer_id": 1005,
            "customer_name": "Omega General",
            "region": "North",
            "customer_segment": "Traditional"
        }
    ]

    # 🔥 Force schema to match Iceberg table (int32 not int64)
    desired_schema = pa.schema([
        ("customer_id", pa.int32()),
        ("customer_name", pa.string()),
        ("region", pa.string()),
        ("customer_segment", pa.string())
    ])

    arrow_table = pa.Table.from_pylist(new_rows, schema=desired_schema)

    table.append(arrow_table)
    print("Inserted new schema records.")

    snap = table.current_snapshot()
    print("Current Snapshot:", snap.snapshot_id)

if __name__ == "__main__":
    main()
