from yoda_dbt2looker import generator, models
from unittest.mock import MagicMock, patch, call


def test__convert_all_refs_to_relation_name():
    result = generator._convert_all_refs_to_relation_name("ref('some_model')")
    assert result == "some_model"
    result = generator._convert_all_refs_to_relation_name("'some_model'")
    assert result == "'some_model'"
    result = generator._convert_all_refs_to_relation_name(
        " ${ref('model1').key1} = ${ref('model2').key2}"
    )
    assert result == "${model1.key1}  =  ${model2.key2}"
    result = generator._convert_all_refs_to_relation_name(
        " ${ref('model1').key1} = ${ref('model2').key2} and ${ref('model1').key2} = ${ref('model3').key1}"
    )
    assert (
        result
        == "${model1.key1}  =  ${model2.key2} and ${model1.key2}  =  ${model3.key1}"
    )
    result = generator._convert_all_refs_to_relation_name(
        " (${ref('model1').key1} = ${ref('model2').key2} and ${ref('model1').key2} = ${ref('model3').key1}) or (${ref('model4').key1} = ${ref('model5').key2} or ${ref('model6').key2} = ${ref('model7').key1})"
    )
    assert (
        result
        == "(${model1.key1}  =  ${model2.key2} and ${model1.key2}  =  ${model3.key1} )or( ${model4.key1}  =  ${model5.key2} or ${model6.key2}  =  ${model7.key1})"
    )


def test__generate_dimensions_no_column_enabled_returns_empty_list():
    model = MagicMock()
    column = MagicMock()
    model.columns = {"col1": column}
    column.meta.dimension.enabled = False
    assert generator._generate_dimensions(model, None) == []


@patch("yoda_dbt2looker.generator.map_adapter_type_to_looker")
def test__generate_dimensions_column_enabled_not_scalar_type_returns_empty_list(
    map_adapter_type_to_looker_mock,
):
    model = MagicMock()
    column = MagicMock()
    model.columns = {"col1": column}
    column.meta.dimension.enabled = True
    data_type = MagicMock()
    column.data_type = data_type
    map_adapter_type_to_looker_mock.return_value = "date"
    adapter_type = MagicMock()
    assert generator._generate_dimensions(model, adapter_type) == []
    assert map_adapter_type_to_looker_mock.mock_calls == [call(adapter_type, data_type)]


@patch("yoda_dbt2looker.generator.map_adapter_type_to_looker")
def test__generate_dimensions_column_enabled_has_sql_returns_dimension(
    map_adapter_type_to_looker_mock,
):
    model = MagicMock()
    column = MagicMock()
    model.columns = {"col1": column}
    column.meta.dimension.enabled = True
    data_type = MagicMock()
    column.data_type = data_type
    map_adapter_type_to_looker_mock.return_value = "number"
    adapter_type = MagicMock()
    column.meta.dimension.name = "col1"
    column.meta.dimension.sql = "col1 sql"
    column.meta.dimension.description = "col1 description"
    column.meta.dimension.value_format_name = None
    assert generator._generate_dimensions(model, adapter_type) == [
        {
            "name": "col1",
            "type": "number",
            "sql": "col1 sql",
            "description": "col1 description",
        }
    ]
    assert map_adapter_type_to_looker_mock.mock_calls == [
        call(adapter_type, data_type),
        call(adapter_type, data_type),
    ]


@patch("yoda_dbt2looker.generator.map_adapter_type_to_looker")
def test__generate_dimensions_column_enabled_no_sql_returns_dimension(
    map_adapter_type_to_looker_mock,
):
    model = MagicMock()
    column = MagicMock()
    model.columns = {"col1": column}
    column.meta.dimension.enabled = True
    data_type = MagicMock()
    column.data_type = data_type
    map_adapter_type_to_looker_mock.return_value = "number"
    adapter_type = MagicMock()
    column.meta.dimension.name = None
    column.name = "col1"
    column.meta.dimension.sql = None
    column.meta.dimension.description = "col1 description"
    column.meta.dimension.value_format_name = None
    assert generator._generate_dimensions(model, adapter_type) == [
        {
            "name": "col1",
            "type": "number",
            "sql": f"${{TABLE}}.col1",
            "description": "col1 description",
        }
    ]
    assert map_adapter_type_to_looker_mock.mock_calls == [
        call(adapter_type, data_type),
        call(adapter_type, data_type),
    ]


@patch("yoda_dbt2looker.generator.map_adapter_type_to_looker")
def test__generate_dimensions_column_enabled_is_primary_key_returns_dimension(
    map_adapter_type_to_looker_mock,
):
    model = MagicMock()
    column = MagicMock()
    model.columns = {"col1": column}
    column.meta.dimension.enabled = True
    data_type = MagicMock()
    column.data_type = data_type
    map_adapter_type_to_looker_mock.return_value = "number"
    adapter_type = MagicMock()
    column.meta.dimension.name = None
    column.name = "col1"
    column.meta.dimension.sql = None
    column.meta.dimension.description = "col1 description"
    column.meta.dimension.value_format_name = None
    model.meta.primary_key = "col1"
    assert generator._generate_dimensions(model, adapter_type) == [
        {
            "name": "col1",
            "type": "number",
            "sql": f"${{TABLE}}.col1",
            "description": "col1 description",
            "primary_key": "yes",
        }
    ]
    assert map_adapter_type_to_looker_mock.mock_calls == [
        call(adapter_type, data_type),
        call(adapter_type, data_type),
    ]


@patch("yoda_dbt2looker.generator.map_adapter_type_to_looker")
def test__generate_dimensions_column_enabled_col_has_value_format_name_but_not_number(
    map_adapter_type_to_looker_mock,
):
    model = MagicMock()
    column = MagicMock()
    model.columns = {"col1": column}
    column.meta.dimension.enabled = True
    data_type = MagicMock()
    column.data_type = data_type
    map_adapter_type_to_looker_mock.return_value = "yesno"
    adapter_type = MagicMock()
    column.meta.dimension.name = None
    column.name = "col1"
    column.meta.dimension.sql = None
    column.meta.dimension.description = "col1 description"
    column.meta.dimension.value_format_name.value = "format1"
    assert generator._generate_dimensions(model, adapter_type) == [
        {
            "name": "col1",
            "type": "yesno",
            "sql": f"${{TABLE}}.col1",
            "description": "col1 description",
        }
    ]
    assert map_adapter_type_to_looker_mock.mock_calls == [
        call(adapter_type, data_type),
        call(adapter_type, data_type),
        call(adapter_type, data_type),
    ]


@patch("yoda_dbt2looker.generator.map_adapter_type_to_looker")
def test__generate_dimensions_column_enabled_col_has_value_format_name(
    map_adapter_type_to_looker_mock,
):
    model = MagicMock()
    column = MagicMock()
    model.columns = {"col1": column}
    column.meta.dimension.enabled = True
    data_type = MagicMock()
    column.data_type = data_type
    map_adapter_type_to_looker_mock.return_value = "number"
    adapter_type = MagicMock()
    column.meta.dimension.name = None
    column.name = "col1"
    column.meta.dimension.sql = None
    column.meta.dimension.description = "col1 description"
    column.meta.dimension.value_format_name.value = "format1"
    assert generator._generate_dimensions(model, adapter_type) == [
        {
            "name": "col1",
            "type": "number",
            "sql": f"${{TABLE}}.col1",
            "description": "col1 description",
            "value_format_name": "format1",
        }
    ]
    assert map_adapter_type_to_looker_mock.mock_calls == [
        call(adapter_type, data_type),
        call(adapter_type, data_type),
        call(adapter_type, data_type),
    ]

def test__generate_compound_no_primary_key_returns_none():
   model = MagicMock()
   model.meta.primary_key = None
   assert generator._generate_compound_primary_key_if_needed(model) is None

def test__generate_compound_primary_key_not_compound_return_none():
   model = MagicMock()
   model.meta.primary_key = "col1"
   assert generator._generate_compound_primary_key_if_needed(model) is None

def test__generate_compound_primary_key_compound_return_dict():
   model = MagicMock()
   model.meta.primary_key = "col1 , col2"
   assert generator._generate_compound_primary_key_if_needed(model)  == {
            "name": "primary_key",
            "primary_key": "yes",
            "sql": 'CONCAT(${TABLE}.col1,${TABLE}.col2) ',
            "description": f"auto generated compound key from the columns:col1 , col2",
        }

@patch("yoda_dbt2looker.generator._generate_compound_primary_key_if_needed")
@patch("yoda_dbt2looker.generator._generate_dimensions")
def test_lookml_dimensions_from_model_no_compound_key_return_only_dimensions(generate_dimensions_mock , generate_compound_primary_key_if_needed_mock):
   dimension1 = MagicMock()
   generate_dimensions_mock.return_value = [dimension1]
   generate_compound_primary_key_if_needed_mock.return_value = None
   model = MagicMock()
   adapter_type = MagicMock()
   assert generator.lookml_dimensions_from_model(model , adapter_type) == [dimension1] 
   assert generate_dimensions_mock.mock_calls == [call(model , adapter_type)]
   assert generate_compound_primary_key_if_needed_mock.mock_calls == [call(model)]

@patch("yoda_dbt2looker.generator._generate_compound_primary_key_if_needed")
@patch("yoda_dbt2looker.generator._generate_dimensions")
def test_lookml_dimensions_from_model_has_compound_key_return_joined_list(generate_dimensions_mock , generate_compound_primary_key_if_needed_mock):
   dimension1 = MagicMock()
   dimension2 = MagicMock()
   generate_dimensions_mock.return_value = [dimension1]
   generate_compound_primary_key_if_needed_mock.return_value = dimension2
   model = MagicMock()
   adapter_type = MagicMock()
   assert generator.lookml_dimensions_from_model(model , adapter_type) == [dimension1 , dimension2] 
   assert generate_dimensions_mock.mock_calls == [call(model , adapter_type)]
   assert generate_compound_primary_key_if_needed_mock.mock_calls[0] == call(model) 

def test_looker_inner_on_column_meta():
    columns = dict()
    columns["col_name"] = models.DbtModelColumn(name="test", description="", meta=models.DbtModelColumnMeta())
    columns["col_name"].meta.looker = models.Dbt2InnerLookerMeta()
    
    columns["col_name"].meta.looker.measures = {}
    columns["col_name"].meta.looker.measures["one"] = models.Dbt2LookerMeasure(type = models.LookerAggregateMeasures.average, description="test measure", sql="a=b")
        
    model_meta = models.DbtModelMeta()
    model: models.DbtModel = models.DbtModel(unique_id="a", resource_type="model", relation_name="", schema="", name="test", description="", tags=[], columns=columns, meta= model_meta)
    model.name = "test"    

    value = generator.lookml_measures_from_model(model)
    assert value == [{'name': 'one', 'type': 'average', 'description': 'test measure', 'sql': 'a=b'}, {'name': 'count', 'type': 'count', 'description': 'Default count measure'}]

def test_main_explorer():
    columns = dict()
    columns["col_name"] = models.DbtModelColumn(name="test", description="", meta=models.DbtModelColumnMeta())
    columns["col_name"].meta.looker = models.Dbt2InnerLookerMeta()
    
    columns["col_name"].meta.looker.measures = {"one": models.Dbt2LookerMeasure(type = models.LookerAggregateMeasures.average, description="test measure", sql="a=b")}
        
    model_meta = models.DbtModelMeta()
    model_meta.looker = models.Dbt2MetaLookerModelMeta(main_model = "ref('main_abc')" , connection="connection1")
    model_meta.looker.joins = [models.Dbt2LookerExploreJoin(join = "test_join", sql_on="field")]
    model: models.DbtModel = models.DbtModel(unique_id="a", resource_type="model", relation_name="", schema="", name="test", description="", tags=[], columns=columns, meta= model_meta)
    model.name = "test"    
    value = generator.lookml_model_data_from_dbt_model(model, "project")
    assert value == 'connection: "connection1"\ninclude: "views/*"\n\nexplore: main_abc {\n  description: ""\n\n  join: test_join {\n    type: left_outer\n    relationship: many_to_one\n    sql_on: field ;;\n  }\n}'