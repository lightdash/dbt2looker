from . import models
from google.cloud import bigquery
from google.cloud import storage

client =  bigquery.Client.from_service_account_json('service_account.json')

def extract_schema(client, model: models.DBTModel):
    project = project.name
    dataset_id = model.schema
    table_id = model.name

    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)  # API Request

    return ["{0} {1}".format(schema.name,schema.field_type) for schema in table.schema] # returns: ['word STRING', 'word_count INTEGER']


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
