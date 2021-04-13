from . import models

def lookml_view_from_dbt_model(model: models.DBTModel):
    return models.LookViewFile(
        filename='test.view',
        contents='test test test',
    )
