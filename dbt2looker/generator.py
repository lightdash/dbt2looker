from . import models

def lookml_view_from_dbt_model(model: models.DbtModel):
    return models.LookViewFile(
        filename='test.view',
        contents='test test test',
    )
