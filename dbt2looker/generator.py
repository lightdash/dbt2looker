from typing import List

import lkml

from . import models

map_to_looker_dtypes = {
    'bigquery': {
        "INTEGER":   "number",
        "FLOAT":     "number",
        "NUMERIC":   "number",
        "BOOLEAN":   "yesno",
        "STRING":    "string",
        "TIMESTAMP": "timestamp",
        "DATETIME":  "datetime",
        "DATE":      "date",
        "TIME":      "string",    # Can time-only be handled better in looker?
        "BOOL":      "yesno",
        "ARRAY":     "string",
        "GEOGRAPHY": "string",
    }
}

looker_date_time_types = ['datetime', 'timestamp']
looker_date_types = ['date']
looker_scalar_types = ['number', 'yesno', 'string']

looker_timeframes = [
    'raw',
    'time',
    'date',
    'week',
    'month',
    'quarter',
    'year',
]


def lookml_date_time_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters):
    return {
        'name': column.name,
        'type': 'time',
        'sql': f'${{TABLE}}.{column.name}',
        'description': column.description,
        'datatype': map_to_looker_dtypes[adapter_type][column.data_type],
        'timeframes': ['raw', 'time', 'hour', 'date', 'week', 'month', 'quarter', 'year']
    }


def lookml_date_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters):
    return {
        'name': column.name,
        'type': 'time',
        'sql': f'${{TABLE}}.{column.name}',
        'description': column.description,
        'datatype': map_to_looker_dtypes[adapter_type][column.data_type],
        'timeframes': ['raw', 'date', 'week', 'month', 'quarter', 'year']
    }


def lookml_dimension_groups_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    date_times = [
        lookml_date_time_dimension_group(column, adapter_type)
        for column in model.columns.values()
        if map_to_looker_dtypes[adapter_type][column.data_type] in looker_date_time_types
    ]
    dates = [
        lookml_date_dimension_group(column, adapter_type)
        for column in model.columns.values()
        if map_to_looker_dtypes[adapter_type][column.data_type] in looker_date_types
    ]
    return date_times + dates


def lookml_dimensions_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    return [
        {
            'name': column.name,
            'type': map_to_looker_dtypes[adapter_type][column.data_type],
            'sql': f'${{TABLE}}.{column.name}',
            'description': column.description
        }
        for column in model.columns.values()
        if map_to_looker_dtypes[adapter_type][column.data_type] in looker_scalar_types
    ]


def lookml_view_from_dbt_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    lookml = {
        'view': {
            'name': model.name,
            'sql_table_name': f'{model.database}.{model.db_schema}.{model.name}',
            'dimension_groups': lookml_dimension_groups_from_model(model, adapter_type),
            'dimensions': lookml_dimensions_from_model(model, adapter_type),
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
