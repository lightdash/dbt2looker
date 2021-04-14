from typing import Dict, List

import lkml

from . import models


def get_column_type_from_catalog(catalog_nodes: Dict[str, models.DbtCatalogNode], model_id: str, column_name: str):
    node = catalog_nodes.get(model_id)
    column = None if node is None else node.columns.get(column_name)
    return None if column is None else column.type


def lookml_view_from_dbt_model(model: models.DbtModel, catalog_nodes: Dict[str, models.DbtCatalogNode]):
    lookml = {
        'view': {
            'name': model.name,
            'sql_table_name': f'{model.database}.{model.db_schema}.{model.name}',
            'dimensions': [
                {
                    'name': column.name,
                    'type': get_column_type_from_catalog(catalog_nodes, model.unique_id, column.name) or 'string',
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


def lookml_model_from_dbt_project(dbt_models: List[models.DbtModel]):
    lookml = {
        'connection': 'your_connection_name', # fix
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
    filename = f'your_connection_name.model' # fix
    return models.LookModelFile(filename=filename, contents=contents)
