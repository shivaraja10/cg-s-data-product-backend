# app/main.py
from fastapi import FastAPI
from app.models import (
    CreateTableRequest, AddColumnRequest,
    InsertRequest, UpdateRequest
)
from app.iceberg_service import (
    create_table, add_column,
    insert_rows, update_rows,
    get_schema, read_table
)

app = FastAPI(title="Iceberg FastAPI Service")


@app.get("/")
def home():
    return {"status": "Iceberg API Running"}


@app.post("/create-table")
def api_create_table(req: CreateTableRequest):
    create_table(req.namespace, req.table, req.schema)
    return {"message": "Table created successfully"}


@app.post("/add-column")
def api_add_column(req: AddColumnRequest):
    add_column(req.namespace, req.table, req.column_name, req.column_type)
    return {"message": "Column added successfully"}


@app.post("/insert")
def api_insert(req: InsertRequest):
    insert_rows(req.namespace, req.table, req.records)
    return {"message": "Rows inserted successfully"}


@app.post("/update")
def api_update(req: UpdateRequest):
    update_rows(req.namespace, req.table, req.filter_column, req.filter_value, req.update_values)
    return {"message": "Rows updated successfully"}


@app.get("/schema/{namespace}/{table}")
def api_get_schema(namespace: str, table: str):
    schema = get_schema(namespace, table)
    return {"schema": str(schema)}


@app.get("/read/{namespace}/{table}")
def api_read(namespace: str, table: str):
    data = read_table(namespace, table)
    return {"data": data}
