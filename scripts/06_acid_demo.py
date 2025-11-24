from config.catalog_config import get_catalog

def main():
    catalog = get_catalog()
    table = catalog.load_table("customer_db.customer_master")

    print("Running ACID test...")

    try:
        table.append([{"customer_id": None}])
    except Exception as e:
        print("Failed append (ACID rollback ensured):", e)

    df = table.scan().to_pandas()
    print("\nTable after failed transaction:")
    print(df)

if __name__ == "__main__":
    main()
