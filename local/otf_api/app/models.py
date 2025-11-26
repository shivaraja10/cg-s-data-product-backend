# app/models.py
from pydantic import BaseModel
from typing import Dict, Any, List

class CreateTableRequest(BaseModel):
    namespace: str
    table: str
    schema: Dict[str, str]

class AddColumnRequest(BaseModel):
    namespace: str
    table: str
    column_name: str
    column_type: str

class InsertRequest(BaseModel):
    namespace: str
    table: str
    records: List[Dict[str, Any]]

class UpdateRequest(BaseModel):
    namespace: str
    table: str
    filter_column: str
    filter_value: Any
    update_values: Dict[str, Any]