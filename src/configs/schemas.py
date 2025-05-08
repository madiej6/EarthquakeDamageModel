from typing import Dict, List
from pydantic import BaseModel, computed_field
import yaml


class TableSchema(BaseModel):
    schema: Dict[str, str]
    primary_key: List[str]

    @computed_field
    @property
    def duckdb_schema(self) -> str:
        return ", ".join([f"{col} {type}" for col, type in self.schema.items()])

    @computed_field
    @property
    def duckdb_pk(self) -> str:
        return ", ".join(self.primary_key)


class SchemaConfig(BaseModel):
    schemas: Dict[str, TableSchema]

    @staticmethod
    def from_yaml(path: str) -> "SchemaConfig":
        with open(path, "r") as file:
            data_dict = yaml.safe_load(file)
            data = SchemaConfig.model_validate(data_dict)
        return data
