from pyiceberg.catalog.sql import SqlCatalog

def main():
    # Load catalog
    catalog = SqlCatalog(
        name="local",
        uri="sqlite:///catalog.db",
        warehouse="./warehouse"
    )

    table = catalog.load_table("customer_db.customer_master")

    # Snapshots from metadata
    snapshots = table.metadata.snapshots

    print("\n📌 AVAILABLE SNAPSHOTS:")
    for s in snapshots:
        print(f"- Snapshot ID: {s.snapshot_id} | Timestamp: {s.timestamp_ms}")

    # Oldest snapshot
    first_snapshot = snapshots[0]
    print(f"\n⏪ TIME TRAVEL TO SNAPSHOT: {first_snapshot.snapshot_id}")

    # Time travel using scan()
    scan = table.scan(snapshot_id=first_snapshot.snapshot_id)

    arrow_table = scan.to_arrow()
    rows = arrow_table.to_pylist()

    print("\n📄 DATA AT THIS SNAPSHOT:")
    for row in rows:
        print(row)

if __name__ == "__main__":
    main()
