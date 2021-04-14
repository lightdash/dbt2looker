from typing import Dict, List

import lkml

from . import models

map_to_looker_dtypes = {
    'bigquery': {
        "INTEGER":   "number",
        "FLOAT":     "number",
        "NUMERIC":   "number",
        "BOOLEAN":   "yesno",
        "STRING":    "string",
        "TIMESTAMP": "time",
        "DATETIME":  "time",
        "DATE":      "time",
        "TIME":      "time",
        "BOOL":      "yesno",
        "ARRAY":     "string",
        "GEOGRAPHY": "string",
    }
}

looker_timeframes = [
    'raw',
    'time',
    'date',
    'week',
    'month',
    'quarter',
    'year',
]


def get_column_type_from_catalog(catalog_nodes: Dict[str, models.DbtCatalogNode], model_id: str, column_name: str, adapter_type: models.SupportedDbtAdapters):
    node = catalog_nodes.get(model_id)
    column = None if node is None else node.columns.get(column_name)
    return None if column is None else map_to_looker_dtypes[adapter_type].get(column.type)


def lookml_view_from_dbt_model(model: models.DbtModel, catalog_nodes: Dict[str, models.DbtCatalogNode], adapter_type: models.SupportedDbtAdapters):
    lookml = {
        'view': {
            'name': model.name,
            'sql_table_name': f'{model.database}.{model.db_schema}.{model.name}',
            'dimensions': [
                {
                    'name': column.name,
                    'type': get_column_type_from_catalog(catalog_nodes, model.unique_id, column.name, adapter_type) or 'string',
                    'description': column.description,
                    'sql': f'${{TABLE}}.{column.name}'
                }
                for column in model.columns.values()
            ]
        }
    }
    contents = lkml.dump(lookml)
    filename = f'{model.name}.view'
    return models.LookViewFile(filename=filename, contents=contents)


def lookml_model_from_dbt_project(dbt_models: List[models.DbtModel], dbt_project_name: str):
    lookml = {
        'connection': dbt_project_name,
        'include': '/views/*',
        'explores': [
            {
                'name': model.name,
                'description': model.description,
            }
            for model in dbt_models
        ]
    }
    contents = lkml.dump(lookml)
    filename = f'{dbt_project_name}.model'
    return models.LookModelFile(filename=filename, contents=contents)
