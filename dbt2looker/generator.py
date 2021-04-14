import lkml

from . import models


def lookml_view_from_dbt_model(model: models.DbtModel):
    map_bq_to_looker_dtypes = {
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

    timeframe = """
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    """

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
