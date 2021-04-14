from typing import Union, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class LookViewFile(BaseModel):
    filename: str
    contents: str

class LookModelFile(BaseModel):
    filename: str
    contents: str

class DbtModelColumn(BaseModel):
    name: str
    description: str


class DbtNode(BaseModel):
    unique_id: str
    resource_type: str


class DbtModel(DbtNode):
    resource_type: Literal['model']
    database: str
    db_schema: str = Field(..., alias='schema')
    name: str
    description: str
    columns: Dict[str, DbtModelColumn]
    tags: List[str]


class DbtManifest(BaseModel):
    nodes: Dict[str, Union[DbtModel, DbtNode]]


class DbtCatalogNodeMetadata(BaseModel):
    type: str
    database: str
    db_schema: str = Field(..., alias='schema')
    name: str
    comment: Optional[str]
    owner: Optional[str]


class DbtCatalogNodeColumn(BaseModel):
    type: str
    comment: Optional[str]
    index: int
    name: str


class DbtCatalogNode(BaseModel):
    metadata: DbtCatalogNodeMetadata
    columns: Dict[str, DbtCatalogNodeColumn]


class DbtCatalog(BaseModel):
    nodes: Dict[str, DbtCatalogNode]