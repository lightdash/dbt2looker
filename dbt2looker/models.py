from enum import Enum
from typing import Union, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, PydanticValueError, validator


# dbt2looker utility types
class UnsupportedDbtAdapterError(PydanticValueError):
    code = 'unsupported_dbt_adapter'
    msg_template = '{wrong_value} is not a supported dbt adapter'


class SupportedDbtAdapters(str, Enum):
    bigquery = 'bigquery'
    redshift = 'redshift'
    snowflake = 'snowflake'


# Lookml types
class LookerAggregateMeasures(str, Enum):
    average = 'average'
    average_distinct = 'average_distinct'
    count = 'count'
    count_distinct = 'count_distinct'
    list = 'list'
    max = 'max'
    median = 'median'
    median_distinct = 'median_distinct'
    min = 'min'
    percentile = 'percentile'
    percentile_distinct = 'percentile_distinct'
    sum = 'sum'
    sum_distinct = 'sum_distinct'


class Dbt2LookerMeasure(BaseModel):
    name: str
    type: LookerAggregateMeasures


class Dbt2LookerMeta(BaseModel):
    measures: Optional[List[Dbt2LookerMeasure]] = []


# Looker file types
class LookViewFile(BaseModel):
    filename: str
    contents: str


class LookModelFile(BaseModel):
    filename: str
    contents: str


# dbt config types
class DbtProjectConfig(BaseModel):
    name: str


class DbtModelColumnMeta(BaseModel):
    looker: Optional[Dbt2LookerMeta] = Field(Dbt2LookerMeta(), alias='looker.com')


class DbtModelColumn(BaseModel):
    name: str
    description: str
    data_type: Optional[str]
    meta: DbtModelColumnMeta


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