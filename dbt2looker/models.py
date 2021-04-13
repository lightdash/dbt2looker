from typing import Union, Dict, List, Literal
from pydantic import BaseModel, Field


class LookViewFile(BaseModel):
    filename: str
    contents: str


class DbtModelColumn(BaseModel):
    name: str
    description: str


class DbtNode(BaseModel):
    resource_type: str


class DbtModel(DbtNode):
    resource_type: Literal['model']
    database: str
    db_schema: str = Field(..., alias='schema')
    name: str
    description: str
    columns: Dict[str, DbtModelColumn]


class DbtManifest(BaseModel):
    nodes: Dict[str, Union[DbtModel, DbtNode]]
