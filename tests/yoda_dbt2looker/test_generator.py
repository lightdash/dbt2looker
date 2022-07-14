
from yoda_dbt2looker import generator

def test__convert_all_refs_to_relation_name():
   result =  generator._convert_all_refs_to_relation_name("ref('some_model')")
   assert(result == "some_model")
   result =  generator._convert_all_refs_to_relation_name("'some_model'")
   assert(result == "'some_model'")
   result =  generator._convert_all_refs_to_relation_name(" ${ref('model1').key1} = ${ref('model2').key2}")
   assert(result == "${model1.key1}  =  ${model2.key2}")
   result =  generator._convert_all_refs_to_relation_name(" ${ref('model1').key1} = ${ref('model2').key2} and ${ref('model1').key2} = ${ref('model3').key1}")
   assert(result == "${model1.key1}  =  ${model2.key2} and ${model1.key2}  =  ${model3.key1}")
   result =  generator._convert_all_refs_to_relation_name(" (${ref('model1').key1} = ${ref('model2').key2} and ${ref('model1').key2} = ${ref('model3').key1}) or (${ref('model4').key1} = ${ref('model5').key2} or ${ref('model6').key2} = ${ref('model7').key1})")
   assert(result == "(${model1.key1}  =  ${model2.key2} and ${model1.key2}  =  ${model3.key1} )or( ${model4.key1}  =  ${model5.key2} or ${model6.key2}  =  ${model7.key1})")