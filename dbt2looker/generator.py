import logging
from typing import List

import lkml

from . import models

LOOKER_DTYPE_MAP = {
    'bigquery': {
        'INTEGER':   'number',
        'FLOAT':     'number',
        'NUMERIC':   'number',
        'BOOLEAN':   'yesno',
        'STRING':    'string',
        'TIMESTAMP': 'timestamp',
        'DATETIME':  'datetime',
        'DATE':      'date',
        'TIME':      'string',    # Can time-only be handled better in looker?
        'BOOL':      'yesno',
        'ARRAY':     'string',
        'GEOGRAPHY': 'string',
    },
    'snowflake': {
        'NUMBER': 'number',
        'DECIMAL': 'number',
        'NUMERIC': 'number',
        'INT': 'number',
        'INTEGER': 'number',
        'BIGINT': 'number',
        'SMALLINT': 'number',
        'FLOAT': 'number',
        'FLOAT4': 'number',
        'FLOAT8': 'number',
        'DOUBLE': 'number',
        'DOUBLE PRECISION': 'number',
        'REAL': 'number',
        'VARCHAR': 'string',
        'CHAR': 'string',
        'CHARACTER': 'string',
        'STRING': 'string',
        'TEXT': 'string',
        'BINARY': 'string',
        'VARBINARY': 'string',
        'BOOLEAN': 'yesno',
        'DATE': 'date',
        'DATETIME': 'datetime',
        'TIME': 'string',        # can we support time?
        'TIMESTAMP': 'timestamp',
        'TIMESTAMP_NTZ': 'timestamp',
        # TIMESTAMP_LTZ not supported (see https://docs.looker.com/reference/field-params/dimension_group)
        # TIMESTAMP_TZ not supported (see https://docs.looker.com/reference/field-params/dimension_group)
        'VARIANT': 'string',
        'OBJECT': 'string',
        'ARRAY': 'string',
        'GEOGRAPHY': 'string',
    },
    'redshift': {
        'SMALLINT': 'number',
        'INT2': 'number',
        'INTEGER': 'number',
        'INT': 'number',
        'INT4': 'number',
        'BIGINT': 'number',
        'INT8': 'number',
        'DECIMAL': 'number',
        'NUMERIC': 'number',
        'REAL': 'number',
        'FLOAT4': 'number',
        'DOUBLE PRECISION': 'number',
        'FLOAT8': 'number',
        'FLOAT': 'number',
        'BOOLEAN': 'yesno',
        'BOOL': 'yesno',
        'CHAR': 'string',
        'CHARACTER': 'string',
        'NCHAR': 'string',
        'BPCHAR': 'string',
        'VARCHAR': 'string',
        'CHARACTER VARYING': 'string',
        'NVARCHAR': 'string',
        'TEXT': 'string',
        'DATE': 'date',
        'TIMESTAMP': 'timestamp',
        'TIMESTAMP WITHOUT TIME ZONE': 'timestamp',
        # TIMESTAMPTZ not supported
        # TIMESTAMP WITH TIME ZONE not supported
        'GEOMETRY': 'string',
        # HLLSKETCH not supported
        'TIME': 'string',
        'TIME WITHOUT TIME ZONE': 'string',
        # TIMETZ not supported
        # TIME WITH TIME ZONE not supported
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


def map_adapter_type_to_looker(adapter_type: models.SupportedDbtAdapters, column_type: str):
    looker_type = LOOKER_DTYPE_MAP[adapter_type].get(column_type)
    if (column_type is not None) and (looker_type is None):
        logging.warning(f'Column type {column_type} not supported for conversion from {adapter_type} to looker. No dimension will be created.')
    return looker_type


def lookml_date_time_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters):
    return {
        'name': column.name,
        'type': 'time',
        'sql': f'${{TABLE}}.{column.name}',
        'description': column.description,
        'datatype': map_adapter_type_to_looker(adapter_type, column.data_type),
        'timeframes': ['raw', 'time', 'hour', 'date', 'week', 'month', 'quarter', 'year']
    }


def lookml_date_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters):
    return {
        'name': column.name,
        'type': 'time',
        'sql': f'${{TABLE}}.{column.name}',
        'description': column.description,
        'datatype': map_adapter_type_to_looker(adapter_type, column.data_type),
        'timeframes': ['raw', 'date', 'week', 'month', 'quarter', 'year']
    }


def lookml_dimension_groups_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    date_times = [
        lookml_date_time_dimension_group(column, adapter_type)
        for column in model.columns.values()
        if map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_time_types
    ]
    dates = [
        lookml_date_dimension_group(column, adapter_type)
        for column in model.columns.values()
        if map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_types
    ]
    return date_times + dates


def lookml_dimensions_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    return [
        {
            'name': column.name,
            'type': map_adapter_type_to_looker(adapter_type, column.data_type),
            'sql': f'${{TABLE}}.{column.name}',
            'description': column.description
        }
        for column in model.columns.values()
        if map_adapter_type_to_looker(adapter_type, column.data_type) in looker_scalar_types
    ]


def lookml_measures_from_model(model: models.DbtModel):
    return [
        {
            'name': measure.name,
            'type': measure.type.value,
            'sql': f'${{TABLE}}.{column.name}',
            'description': f'{measure.type.value.capitalize()} of {column.description}',
        }
        for column in model.columns.values()
        for measure in column.meta.looker.measures
    ]


def lookml_view_from_dbt_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    lookml = {
        'view': {
            'name': model.name,
            'sql_table_name': f'{model.database}.{model.db_schema}.{model.name}',
            'dimension_groups': lookml_dimension_groups_from_model(model, adapter_type),
            'dimensions': lookml_dimensions_from_model(model, adapter_type),
            'measures': lookml_measures_from_model(model),
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
