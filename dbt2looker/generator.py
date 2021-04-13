from . import models

def lookml_view_from_dbt_model(model: models.DBTModel):
    contents = ''
    for column in model.columns:
        column_vals = column_vals + """
            dimension: {} {
              description: {}
              type: string # need to change type to be pulled from BQ schema
              sql: ${TABLE}.{};;
            }
        """.format(model.name, column.description, model.name)

    return models.LookViewFile(
        filename='{}.view'.format(model.name),
        contents= """
        view: {} {
          sql_table_name: `{}.{}` ;;

        """.format(model.name, model.schema, model.name) + column_vals + "\n}",
    )
