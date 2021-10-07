from enum import Enum
from typing import Union, Dict, List, Optional
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
from pydantic import BaseModel, Field, PydanticValueError, validator


# dbt2looker utility types
class UnsupportedDbtAdapterError(PydanticValueError):
    code = 'unsupported_dbt_adapter'
    msg_template = '{wrong_value} is not a supported dbt adapter'


class SupportedDbtAdapters(str, Enum):
    bigquery = 'bigquery'
    postgres = 'postgres'
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
    # percentile = 'percentile'
    # percentile_distinct = 'percentile_distinct'
    sum = 'sum'
    sum_distinct = 'sum_distinct'


class LookerJoinType(str, Enum):
    left_outer = 'left_outer'
    full_outer = 'full_outer'
    inner = 'inner'
    cross = 'cross'


class LookerJoinRelationship(str, Enum):
    many_to_one = 'many_to_one'
    many_to_many = 'many_to_many'
    one_to_many = 'one_to_many'
    one_to_one = 'one_to_one'


class LookerValueFormatName(str, Enum):
    decimal_0 = 'decimal_0'
    decimal_1 = 'decimal_1'
    decimal_2 = 'decimal_2'
    decimal_3 = 'decimal_3'
    decimal_4 = 'decimal_4'
    usd_0 = 'usd_0'
    usd = 'usd'
    gbp_0 = 'gbp_0'
    gbp = 'gbp'
    eur_0 = 'eur_0'
    eur = 'eur'
    id = 'id'
    percent_0 = 'percent_0'
    percent_1 = 'percent_1'
    percent_2 = 'percent_2'
    percent_3 = 'percent_3'
    percent_4 = 'percent_4'


class Dbt2LookerMeasure(BaseModel):
    type: LookerAggregateMeasures
    filters: Optional[List[Dict[str, str]]] = []
    description: Optional[str]
    sql: Optional[str]
    value_format_name: Optional[LookerValueFormatName]

    @validator('filters')
    def filters_are_singular_dicts(cls, v: List[Dict[str, str]]):
        if v is not None:
            for f in v:
                if len(f) != 1:
                    raise ValueError('Multiple filter names provided for a single filter in measure block')
        return v


class Dbt2LookerDimension(BaseModel):
    enabled: Optional[bool] = True
    name: Optional[str]
    sql: Optional[str]
    description: Optional[str]
    value_format_name: Optional[LookerValueFormatName]


class Dbt2LookerMeta(BaseModel):
    measures: Optional[Dict[str, Dbt2LookerMeasure]] = {}
    measure: Optional[Dict[str, Dbt2LookerMeasure]] = {}
    metrics: Optional[Dict[str, Dbt2LookerMeasure]] = {}
    metric: Optional[Dict[str, Dbt2LookerMeasure]] = {}
    dimension: Optional[Dbt2LookerDimension] = Dbt2LookerDimension()


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


class DbtModelColumnMeta(Dbt2LookerMeta):
    pass


class DbtModelColumn(BaseModel):
    name: str
    description: str
    data_type: Optional[str]
    meta: DbtModelColumnMeta


class DbtNode(BaseModel):
    unique_id: str
    resource_type: str


class Dbt2LookerExploreJoin(BaseModel):
    join: str
    type: Optional[LookerJoinType] = LookerJoinType.left_outer
    relationship: Optional[LookerJoinRelationship] = LookerJoinRelationship.many_to_one
    sql_on: str


class Dbt2LookerModelMeta(BaseModel):
    joins: Optional[List[Dbt2LookerExploreJoin]] = []


class DbtModelMeta(Dbt2LookerModelMeta):
    pass


class DbtModel(DbtNode):
    resource_type: Literal['model']
    database: str
    db_schema: str = Field(..., alias='schema')
    name: str
    description: str
    columns: Dict[str, DbtModelColumn]
    tags: List[str]
    meta: DbtModelMeta

    @validator('columns')
    def case_insensitive_column_names(cls, v: Dict[str, DbtModelColumn]):
        return {
            name.lower(): column.copy(update={'name': column.name.lower()})
            for name, column in v.items()
        }


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

    @validator('columns')
    def case_insensitive_column_names(cls, v: Dict[str, DbtCatalogNodeColumn]):
        return {
            name.lower(): column.copy(update={'name': column.name.lower()})
            for name, column in v.items()
        }


class DbtCatalog(BaseModel):
    nodes: Dict[str, DbtCatalogNode]