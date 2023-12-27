from unittest.mock import MagicMock
from yoda_dbt2looker import models
from yoda_dbt2looker.parser import (
    _extract_measures_models,
    _extract_exposure_models,
)


def test__extract_measures_models():
    exposure_model_views = set()
    exposure1 = MagicMock()
    model_to_measure = {}
    exposure1.meta.looker.measures = [
        models.Dbt2LookerExploreMeasure(
            name="measure_1",
            model="ref('model_1')",
            sql="(SUM(${ref('model_1').interacted_users}) / SUM(${ref('model_1').total_users})",
            description="",
            type=models.LookerNoneAggregateMeasures.number.value,
        ),
        models.Dbt2LookerExploreMeasure(
            name="measure_2",
            model="ref('model_2')",
            sql="(SUM(${ref('model_2').interacted_users}) / SUM(${ref('model_1').total_users})",
            description="",
            type=models.LookerNoneAggregateMeasures.number.value,
        ),
    ]
    _extract_measures_models(exposure_model_views, model_to_measure, exposure1)
    assert exposure_model_views == {"model_1", "model_2"}
    assert model_to_measure == {
        "model_1": [exposure1.meta.looker.measures[0]],
        "model_2": [exposure1.meta.looker.measures[1]],
    }


def test__extract_models():
    exposure_model_views = set()
    exposure = MagicMock()
    model = {}
    exposure.meta.looker.dimensions = [
        models.Dbt2LookerExploreDimension(
            name="calculated_dimension_1",
            model="ref('model_1')",
            sql="case when ${col1} is null then ${col2} else ${col4} end",
            description="",
            type="number",
        ),
        models.Dbt2LookerExploreDimension(
            name="measure_2",
            model="ref('model_2')",
            sql="case when ${col1} is null then ${col2} else ${col1} end",
            description="",
            type="string",
        ),
    ]
    _extract_exposure_models(exposure_model_views, model, exposure.meta.looker.dimensions)
    assert exposure_model_views == {"model_1", "model_2"}
    assert model == {
        "model_1": [exposure.meta.looker.dimensions[0]],
        "model_2": [exposure.meta.looker.dimensions[1]],
    }
