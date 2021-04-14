import lkml

from . import models


def lookml_view_from_dbt_model(model: models.DbtModel):
    lookml = {
        'view': {
            'name': model.name,
            'sql_table_name': f'{model.database}.{model.db_schema}.{model.name}',
            'dimensions': [
                {
                    'name': column.name,
                    'type': 'string',       # fix
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

def lookml_model_from_dbt_project(models: List[models.DbtModel]):
    lookml = {
        'connection': 'your_connection_name' # fix
        'include': '"/views/*"'
        [
            'explore': f'model.name {}'
            for model in models
        ]
    }
    contents = lkml.dump(lookml)
    filename = f'your_connection_name.model' # fix
    return models.LookModelFile(filename=filename, contents=contents)
