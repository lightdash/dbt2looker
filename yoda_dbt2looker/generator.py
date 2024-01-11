import logging
import re
from pathlib import Path
from typing import Optional
import lkml

from . import models

LOOKER_DTYPE_MAP = {
    "bigquery": {
        "INT64": "number",
        "INTEGER": "number",
        "FLOAT": "number",
        "FLOAT64": "number",
        "NUMERIC": "number",
        "BOOLEAN": "yesno",
        "STRING": "string",
        "TIMESTAMP": "timestamp",
        "DATETIME": "datetime",
        "DATE": "date",
        "TIME": "string",  # Can time-only be handled better in looker?
        "BOOL": "yesno",
        "ARRAY": "string",
        "GEOGRAPHY": "string",
    },
    "snowflake": {
        "NUMBER": "number",
        "DECIMAL": "number",
        "NUMERIC": "number",
        "INT": "number",
        "INTEGER": "number",
        "BIGINT": "number",
        "SMALLINT": "number",
        "FLOAT": "number",
        "FLOAT4": "number",
        "FLOAT8": "number",
        "DOUBLE": "number",
        "DOUBLE PRECISION": "number",
        "REAL": "number",
        "VARCHAR": "string",
        "CHAR": "string",
        "CHARACTER": "string",
        "STRING": "string",
        "TEXT": "string",
        "BINARY": "string",
        "VARBINARY": "string",
        "BOOLEAN": "yesno",
        "DATE": "date",
        "DATETIME": "datetime",
        "TIME": "string",  # can we support time?
        "TIMESTAMP": "timestamp",
        "TIMESTAMP_NTZ": "timestamp",
        # TIMESTAMP_LTZ not supported (see https://docs.looker.com/reference/field-params/dimension_group)
        # TIMESTAMP_TZ not supported (see https://docs.looker.com/reference/field-params/dimension_group)
        "VARIANT": "string",
        "OBJECT": "string",
        "ARRAY": "string",
        "GEOGRAPHY": "string",
    },
    "redshift": {
        "SMALLINT": "number",
        "INT2": "number",
        "INTEGER": "number",
        "INT": "number",
        "INT4": "number",
        "BIGINT": "number",
        "INT8": "number",
        "DECIMAL": "number",
        "NUMERIC": "number",
        "REAL": "number",
        "FLOAT4": "number",
        "DOUBLE PRECISION": "number",
        "FLOAT8": "number",
        "FLOAT": "number",
        "BOOLEAN": "yesno",
        "BOOL": "yesno",
        "CHAR": "string",
        "CHARACTER": "string",
        "NCHAR": "string",
        "BPCHAR": "string",
        "VARCHAR": "string",
        "CHARACTER VARYING": "string",
        "NVARCHAR": "string",
        "TEXT": "string",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "TIMESTAMP WITHOUT TIME ZONE": "timestamp",
        # TIMESTAMPTZ not supported
        # TIMESTAMP WITH TIME ZONE not supported
        "GEOMETRY": "string",
        # HLLSKETCH not supported
        "TIME": "string",
        "TIME WITHOUT TIME ZONE": "string",
        # TIMETZ not supported
        # TIME WITH TIME ZONE not supported
    },
    "postgres": {
        # BIT, BIT VARYING, VARBIT not supported
        # BOX not supported
        # BYTEA not supported
        # CIRCLE not supported
        # INTERVAL not supported
        # LINE not supported
        # LSEG not supported
        # PATH not supported
        # POINT not supported
        # POLYGON not supported
        # TSQUERY, TSVECTOR not supported
        "XML": "string",
        "UUID": "string",
        "PG_LSN": "string",
        "MACADDR": "string",
        "JSON": "string",
        "JSONB": "string",
        "CIDR": "string",
        "INET": "string",
        "MONEY": "number",
        "SMALLINT": "number",
        "INT2": "number",
        "SMALLSERIAL": "number",
        "SERIAL2": "number",
        "INTEGER": "number",
        "INT": "number",
        "INT4": "number",
        "SERIAL": "number",
        "SERIAL4": "number",
        "BIGINT": "number",
        "INT8": "number",
        "BIGSERIAL": "number",
        "SERIAL8": "number",
        "DECIMAL": "number",
        "NUMERIC": "number",
        "REAL": "number",
        "FLOAT4": "number",
        "DOUBLE PRECISION": "number",
        "FLOAT8": "number",
        "FLOAT": "number",
        "BOOLEAN": "yesno",
        "BOOL": "yesno",
        "CHAR": "string",
        "CHARACTER": "string",
        "NCHAR": "string",
        "BPCHAR": "string",
        "VARCHAR": "string",
        "CHARACTER VARYING": "string",
        "NVARCHAR": "string",
        "TEXT": "string",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "TIMESTAMP WITHOUT TIME ZONE": "timestamp",
        # TIMESTAMPTZ not supported
        # TIMESTAMP WITH TIME ZONE not supported
        "GEOMETRY": "string",
        # HLLSKETCH not supported
        "TIME": "string",
        "TIME WITHOUT TIME ZONE": "string",
        # TIMETZ not supported
        # TIME WITH TIME ZONE not supported
    },
    "spark": {
        "byte": "number",
        "short": "number",
        "integer": "number",
        "long": "number",
        "float": "number",
        "double": "number",
        "decimal": "number",
        "string": "string",
        "varchar": "string",
        "char": "string",
        "boolean": "yesno",
        "timestamp": "timestamp",
        "date": "datetime",
    },
}

looker_date_time_types = ["datetime", "timestamp"]
looker_date_types = ["date"]
looker_scalar_types = ["number", "yesno", "string"]

looker_timeframes = [
    "raw",
    "time",
    "date",
    "week",
    "month",
    "quarter",
    "year",
]


def normalise_spark_types(column_type: str) -> str:
    return re.match(r"^[^\(]*", column_type).group(0)


def map_adapter_type_to_looker(
    adapter_type: models.SupportedDbtAdapters, column_type: str
):
    if column_type is None:
        return None
    normalised_column_type = (
        normalise_spark_types(column_type)
        if adapter_type == models.SupportedDbtAdapters.spark.value
        else column_type
    )
    looker_type = LOOKER_DTYPE_MAP[adapter_type].get(normalised_column_type)
    if (column_type is not None) and (looker_type is None):
        logging.warning(
            f"Column type {column_type} not supported for conversion from {adapter_type} to looker. No dimension will be created."
        )
    return looker_type


def lookml_date_time_dimension_group(
    column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters
):
    return {
        "name": column.meta.dimension.name or column.name,
        "type": "time",
        "sql": column.meta.dimension.sql or f"${{TABLE}}.{column.name}",
        "description": column.meta.dimension.description or column.description,
        "datatype": map_adapter_type_to_looker(adapter_type, column.data_type),
        "timeframes": [
            "raw",
            "time",
            "hour",
            "date",
            "week",
            "month",
            "quarter",
            "year",
        ],
    }


def lookml_date_dimension_group(
    column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters
):
    return {
        "name": column.meta.dimension.name or column.name,
        "type": "time",
        "sql": column.meta.dimension.sql or f"${{TABLE}}.{column.name}",
        "description": column.meta.dimension.description or column.description,
        "datatype": map_adapter_type_to_looker(adapter_type, column.data_type),
        "timeframes": ["raw", "date", "week", "month", "quarter", "year"],
    }


def lookml_dimension_groups_from_model(
    model: models.DbtModel, adapter_type: models.SupportedDbtAdapters
):
    date_times = [
        lookml_date_time_dimension_group(column, adapter_type)
        for column in model.columns.values()
        if map_adapter_type_to_looker(adapter_type, column.data_type)
        in looker_date_time_types
    ]
    dates = [
        lookml_date_dimension_group(column, adapter_type)
        for column in model.columns.values()
        if column.meta.dimension.enabled
        and map_adapter_type_to_looker(adapter_type, column.data_type)
        in looker_date_types
    ]

    exposure_dimension_groups = [
        lookml_exposure_dimension_group_duration(exposure_dimension_group)
        for exposure_dimension_group in model.dimension_groups_exposure
    ]
    return date_times + dates + exposure_dimension_groups


def lookml_dimensions_from_model(
    model: models.DbtModel, adapter_type: models.SupportedDbtAdapters
):
    compound_key = _generate_compound_primary_key_if_needed(model)
    dimensions = _generate_dimensions(model, adapter_type)
    for calculated_dimension in model.calculated_dimension:
        dimensions.append(lookml_calculated_dimension(calculated_dimension))
    if compound_key:
        return dimensions + [compound_key]
    return dimensions


def _generate_dimensions(model, adapter_type):
    return [
        {
            "name": column.meta.dimension.name or column.name,
            "type": map_adapter_type_to_looker(adapter_type, column.data_type),
            "sql": column.meta.dimension.sql or f"${{TABLE}}.{column.name}",
            "description": column.meta.dimension.description or column.description,
            **({"primary_key": "yes"} if model.meta.primary_key == column.name else {}),
            **(
                {"value_format_name": column.meta.dimension.value_format_name.value}
                if (
                    column.meta.dimension.value_format_name
                    and map_adapter_type_to_looker(adapter_type, column.data_type)
                    == "number"
                )
                else {}
            ),
        }
        for column in model.columns.values()
        if column.meta.dimension.enabled
        and map_adapter_type_to_looker(adapter_type, column.data_type)
        in looker_scalar_types
    ]


def _generate_compound_primary_key_if_needed(model: models.DbtModel) -> Optional[dict]:
    if model.meta.primary_key and "," in model.meta.primary_key:
        concat_keys = [
            f"${{TABLE}}.{key.strip()}" for key in model.meta.primary_key.split(",")
        ]
        return {
            "name": "primary_key",
            "primary_key": "yes",
            "sql": f'CONCAT({",".join(concat_keys)}) ',
            "description": f"auto generated compound key from the columns:{model.meta.primary_key}",
        }
    return None


def lookml_measure_filters(measure: models.Dbt2LookerMeasure, model: models.DbtModel):
    try:
        columns = {
            column_name: model.columns[column_name]
            for f in measure.filters
            for column_name in f
        }
    except KeyError as e:
        raise ValueError(
            f"Model {model.unique_id} contains a measure that references a non_existent column: {e}\n"
            f"Ensure that dbt model {model.unique_id} contains a column: {e}"
        ) from e
    return [
        {
            (columns[column_name].meta.dimension.name or column_name): fexpr
            for column_name, fexpr in f.items()
        }
        for f in measure.filters
    ]


def lookml_measures_from_model(model: models.DbtModel):
    measures = [
        lookml_measure(measure_name, column, measure, model)
        for column in model.columns.values()
        for measure_name, measure in {
            **column.meta.looker.measures,
            **column.meta.measures,
            **column.meta.measure,
            **column.meta.metrics,
            **column.meta.metric,
        }.items()
    ]
    for measure in model.measures_exposure:
        measures.append(lookml_exposure_measure(measure))
    measures.append(
        lookml_measure(
            measure_name="count",
            column=None,
            measure=models.Dbt2LookerMeasure(
                type=models.LookerAggregateMeasures.count,
                description="Default count measure",
            ),
            model=None,
        )
    )
    return measures


def lookml_measure(
    measure_name: str,
    column: models.DbtModelColumn,
    measure: models.Dbt2LookerMeasure,
    model: models.DbtModel,
):
    m = {
        "name": measure_name,
        "type": measure.type.value,
        "description": measure.description
        or (column.description if column else None)
        or (f"{measure.type.value.capitalize()} of {column.name}" if column else ""),
    }
    sql = measure.sql or f"${{TABLE}}.{column.name}" if column else None
    if sql:
        m["sql"] = sql
    if measure.filters:
        m["filters"] = lookml_measure_filters(measure, model)
    if measure.value_format_name:
        m["value_format_name"] = measure.value_format_name.value
    return m


def lookml_exposure_measure(measure: models.Dbt2LookerExploreMeasure):
    return {
        "name": measure.name,
        "description": measure.description,
        "type": measure.type.value,
        "sql": _convert_all_refs_to_relation_name(measure.sql , False),
    }


def lookml_calculated_dimension(dimension: models.Dbt2LookerExploreDimension):
    tmp_dimension = {
        "name": dimension.name,
        "description": dimension.description,
        "type": dimension.type.value,
        "sql": _remove_escape_characters(dimension.sql),
    }

    if dimension.hidden:
        tmp_dimension["hidden"] = dimension.hidden

    return tmp_dimension


def lookml_parameter_exposure(exposure_parameter: models.Dbt2LookerExploreParameter):
    tmp_parameter = {
        "name": exposure_parameter.name,
        "type": exposure_parameter.type.value,
        "description": exposure_parameter.description,
    }

    if exposure_parameter.allowed_value:
        tmp_parameter["allowed_value"] = exposure_parameter.allowed_value

    if exposure_parameter.label:
        tmp_parameter["label"] = exposure_parameter.label

    return tmp_parameter


def lookml_filter_exposure(exposure_filter: models.Dbt2LookerExploreFilter):
    tmp_filter = {
        "name": exposure_filter.name,
        "description": exposure_filter.description,
        "type": exposure_filter.type.value,
    }

    if exposure_filter.sql:
        tmp_filter["sql"] = _remove_escape_characters(exposure_filter.sql)

    if exposure_filter.label:
        tmp_filter["label"] = exposure_filter.label

    return tmp_filter


def lookml_exposure_dimension_group_duration(
    dimension_group: models.Dbt2LookerExploreDimensionGroupDuration,
):
    tmp_dimension_group_duration = {
        "name": dimension_group.name,
        "type": dimension_group.type.value,
        "sql_start": _remove_escape_characters(dimension_group.sql_start),
        "sql_end": _remove_escape_characters(dimension_group.sql_end),
        "description": dimension_group.description,
    }

    if dimension_group.datatype:
        tmp_dimension_group_duration["datatype"] = dimension_group.datatype

    if dimension_group.intervals:
        tmp_dimension_group_duration["intervals"] = dimension_group.intervals

    return tmp_dimension_group_duration


def lookml_view_from_dbt_model(
    model: models.DbtModel, adapter_type: models.SupportedDbtAdapters
):
    lookml = {
        "view": {
            "name": model.name,
            "sql_table_name": _get_model_relation_name(model),
            "dimension_groups": lookml_dimension_groups_from_model(model, adapter_type),
            "dimensions": lookml_dimensions_from_model(model, adapter_type),
            "measures": lookml_measures_from_model(model),
        }
    }
    parameters = [
        lookml_parameter_exposure(parameter_exposure)
        for parameter_exposure in model.parameters_exposure
    ]
    filters = [
        lookml_filter_exposure(filter_exposure)
        for filter_exposure in model.filters_exposure
    ]
    if parameters:
        lookml["view"]["parameters"] = parameters
    if filters:
        lookml["view"]["filters"] = filters

    logging.debug(
        f"Created view from model %s with %d measures, %d dimensions",
        model.name,
        len(lookml["view"]["measures"]),
        len(lookml["view"]["dimensions"]),
    )
    try:
        contents = lkml.dump(lookml)
    except Exception as e:
        logging.error(f"Error dumping lookml for model {model.name}")
        raise e
    filename = f"{model.name}.view.lkml"
    return models.LookViewFile(filename=filename, contents=contents)


def _get_model_relation_name(model: models.DbtModel):
    if "yoda_snowflake" in model.tags:
        return f"{model.meta.integration_config.snowflake.properties.sf_schema}.{model.meta.integration_config.snowflake.properties.table}"
    return model.relation_name


def lookml_view_from_dbt_exposure(model: models.DbtModel, dbt_project_name: str):
    pass


# def _convert_all_refs_to_relation_name(manifest: models.DbtManifest, project_name: str, ref_str : str) -> str:
#     reg_ref = r"ref\(\s*\'(\w*)\'\s*\)"
#     matches = re.findall(reg_ref, ref_str)
#     if not matches or len(matches) == 0:
#         return None

#     ref_str = ref_str.replace(" ", "")
#     for group_value in matches:
#         model_loopup = f"model.{project_name}.{group_value.strip()}"
#         model_node = manifest.nodes.get(model_loopup)
#         ref_str = ref_str.replace(f"ref('{group_value}')",model_node.relation_name)
#     ref_str = ref_str.replace("="," = ")

#     return ref_str


def _convert_all_refs_to_relation_name(ref_str: str, handle_spaces: bool = True) -> str:
    reg_ref = r"ref\(\s*\'(\w*)\'\s*\)"
    matches = re.findall(reg_ref, ref_str)
    if not matches or len(matches) == 0:
        return ref_str

    if handle_spaces:
        ref_str = ref_str.replace(" ", "").replace("=", " = ")
    for group_value in matches:
        ref_str = ref_str.replace(f"ref('{group_value}')", group_value)
    # in case of a compound expression with logical operator , i.e : ${join1} and ${join2} - we would like
    # to add a space between the logical operator so all elements between }...$ are captured and added a pre and post space
    ref_str = re.sub(r"}(.*?)\$", r"} \1 $", ref_str)
    return ref_str


def _extract_all_refs(ref_str: str) -> list[str]:
    reg_ref = r"ref\(\s*\'(\w*)\'\s*\)"
    matches = re.findall(reg_ref, ref_str)
    if not matches or len(matches) == 0:
        return None
    refs = []
    ref_str = ref_str.replace(" ", "")
    for group_value in matches:
        refs.append(group_value)

    return refs


def lookml_model_data_from_dbt_model(model: models.DbtModel, dbt_project_name: str):
    # Note: assumes view names = model names
    #       and models are unique across dbt packages in project
    lookml = {
        "connection": dbt_project_name,
        "include": "views/*",
        "explore": {
            "name": model.name,
            "description": model.description,
            "joins": [
                {
                    "name": join.join,
                    "type": join.type.value,
                    "relationship": join.relationship.value,
                    "sql_on": join.sql_on,
                }
                for join in model.meta.joins
            ],
        },
    }
    if model.meta.looker:
        relation_name = _convert_all_refs_to_relation_name(model.meta.looker.main_model)
        if not relation_name:
            logging.error(f"Invalid ref {model.meta.looker.main_model}")

        lookml = {
            "connection": model.meta.looker.connection,
            "include": "views/*",
            "explore": {
                "name": relation_name,
                "description": model.description,
                "joins": [
                    {
                        "name": _convert_all_refs_to_relation_name(join.join),
                        "type": join.type.value,
                        "relationship": join.relationship.value,
                        "sql_on": _convert_all_refs_to_relation_name(join.sql_on),
                    }
                    for join in model.meta.looker.joins
                ],
            },
        }

    if model.meta.looker.sql_always_where:
        lookml["explore"]["sql_always_where"] = _remove_escape_characters(
            _convert_all_refs_to_relation_name(
                model.meta.looker.sql_always_where, False
            )
        )

    return lkml.dump(lookml)


def lookml_model_from_dbt_model(
    manifest: models.DbtManifest, model: models.DbtModel, dbt_project_name: str
):
    contents = lookml_model_data_from_dbt_model(model, dbt_project_name)
    model_loopup = f"exposure.{dbt_project_name}.{model.name}"
    exposure_node = manifest.exposures.get(model_loopup)
    file_name = Path(exposure_node.original_file_path).stem
    filename = f"{file_name}.model.lkml"
    return models.LookModelFile(filename=filename, contents=contents)


def _remove_escape_characters(input_str: str, escape_char: str = "\\") -> str:
    return input_str.replace(escape_char, "")
