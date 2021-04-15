from enum import Enum
from typing import Union, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, PydanticValueError, validator


class UnsupportedDbtAdapterError(PydanticValueError):
    code = 'unsupported_dbt_adapter'
    msg_template = '{wrong_value} is not a supported dbt adapter'


class SupportedDbtAdapters(str, Enum):
    bigquery = 'bigquery'
    redshift = 'redshift'
    snowflake = 'snowflake'


class DbtProjectConfig(BaseModel):
    name: str


class LookViewFile(BaseModel):
    filename: str
    contents: str


class LookModelFile(BaseModel):
    filename: str
    contents: str


class DbtModelColumn(BaseModel):
    name: str
    description: str
    data_type: Optional[str]


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


class DbtManifestMetadata(BaseModel):
    adapter_type: str

    @validator('adapter_type')
    def adapter_must_be_supported(cls, v):
        try:
            SupportedDbtAdapters(v)
        except ValueError:
            raise UnsupportedDbtAdapterError(wrong_value=v)
        return v


class DbtManifest(BaseModel):
    nodes: Dict[str, Union[DbtModel, DbtNode]]
    metadata: DbtManifestMetadata


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