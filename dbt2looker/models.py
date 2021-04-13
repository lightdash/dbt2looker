from typing import Union, List
from pydantic import BaseModel

class LookViewFile(BaseModel):
    filename: string
    contents: string

class DBTManifest(BaseModel):
    nodes: Union[DBTModel, DBTNode]

class DBTNode(BaseModel):
    resource_type: string

class DBTModel(DBTNode):
    resource_type: 'model'
    schema: string
    name: string
    description: string
    columns: List[DBTColumn]

class DBTModelColumn(BaseModel):
    name: string
    description: string
