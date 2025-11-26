# app/iceberg_service.py
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import (
    StringType, IntegerType, DoubleType, BooleanType, TimestampType
)
from app.iceberg_client import catalog
import pandas as pd

# -----------------------------------
# Type mapping
# -----------------------------------
TYPE_MAP = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "bool": BooleanType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
}


def iceberg_type(t: str):
    key = t.lower().strip()
    if key not in TYPE_MAP:
        raise ValueError(f"Unsupported Iceberg type: '{t}'")
    return TYPE_MAP[key]


# -----------------------------------
# Namespace
# -----------------------------------
def ensure_namespace(namespace: str):
    if not catalog.namespace_exists(namespace):
        catalog.create_namespace(namespace)


# -----------------------------------
# Create table
# -----------------------------------
def create_table(namespace: str, table: str, schema_dict: dict):
    ensure_namespace(namespace)

    fields = []
    field_id = 1
    for name, dtype in schema_dict.items():
        fields.append(
            NestedField(field_id, name, iceberg_type(dtype), required=True)
        )
        field_id += 1

    iceberg_schema = Schema(*fields)
    identifier = f"{namespace}.{table}"

    if catalog.table_exists(identifier):
        return catalog.load_table(identifier)

    return catalog.create_table(identifier, schema=iceberg_schema)


# -----------------------------------
# Add column
# -----------------------------------
def add_column(namespace: str, table: str, column_name: str, column_type: str):
    ensure_namespace(namespace)
    tbl = catalog.load_table(f"{namespace}.{table}")
    tbl.update_schema().add_column(column_name, iceberg_type(column_type)).commit()
    return True


# -----------------------------------
# Insert rows
# -----------------------------------
def insert_rows(namespace: str, table: str, rows):
    ensure_namespace(namespace)
    tbl = catalog.load_table(f"{namespace}.{table}")

    df = pd.DataFrame(rows)

    tbl.append(df)
    return True


# -----------------------------------
# Update rows (overwrite)
# -----------------------------------
def update_rows(namespace: str, table: str, filter_column, filter_value, update_values):
    ensure_namespace(namespace)
    tbl = catalog.load_table(f"{namespace}.{table}")

    df = tbl.scan().to_pandas()

    mask = df[filter_column] == filter_value
    for col, val in update_values.items():
        df.loc[mask, col] = val

    tbl.overwrite(df)
    return True


# -----------------------------------
# Schema
# -----------------------------------
def get_schema(namespace: str, table: str):
    ensure_namespace(namespace)
    tbl = catalog.load_table(f"{namespace}.{table}")
    return tbl.schema()


# -----------------------------------
# Read table
# -----------------------------------
def read_table(namespace: str, table: str):
    ensure_namespace(namespace)
    tbl = catalog.load_table(f"{namespace}.{table}")
    return tbl.scan().to_pandas().to_dict(orient="records")