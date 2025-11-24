from config.catalog_config import get_catalog
from pyiceberg.types import StringType

def main():
    catalog = get_catalog()
    table = catalog.load_table("customer_db.customer_master")

    try:
        table.update_schema().add_column("customer_segment", StringType()).commit()
        print("Added column 'customer_segment'.")
    except Exception as e:
        print("Schema evolution failed:", e)

    print("Current schema:")
    print(table.schema())

if __name__ == "__main__":
    main()
