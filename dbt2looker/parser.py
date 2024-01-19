import logging
from typing import Dict, Optional, List
from functools import reduce

from . import models


def parse_dbt_project_config(raw_config: dict):
    return models.DbtProjectConfig(**raw_config)


def parse_catalog_nodes(raw_catalog: dict):
    catalog = models.DbtCatalog(**raw_catalog)
    return catalog.nodes


def parse_adapter_type(raw_manifest: dict):
    manifest = models.DbtManifest(**raw_manifest)
    return manifest.metadata.adapter_type


def tags_match(query_tag: str, model: models.DbtModel) -> bool:
    try:
        return query_tag in model.tags
    except AttributeError:
        return False
    except ValueError:
        # Is the tag just a string?
        return query_tag == model.tags


def parse_models(raw_manifest: dict, tag=None) -> List[models.DbtModel]:
    manifest = models.DbtManifest(**raw_manifest)
    materialized_models: List[models.DbtModel] = [
        node
        for node in manifest.nodes.values()
        if node.resource_type == 'model' and node.config['materialized'] != 'ephemeral'
    ]

    if tag is None:
        selected_models = materialized_models
    else:
        selected_models = [model for model in materialized_models if tags_match(tag, model)]

    # Empty model files have many missing parameters
    for model in selected_models:
        if not hasattr(model, 'name'):
            logging.error('Cannot parse model with id: "%s" - is the model file empty?', model.unique_id)
            raise SystemExit('Failed')

    return selected_models


def check_models_for_missing_column_types(dbt_typed_models: List[models.DbtModel]):
    for model in dbt_typed_models:
        if all([col.data_type is None for col in model.columns.values()]):
            logging.debug('Model %s has no typed columns, no dimensions will be generated. %s', model.unique_id, model)


def compare_model_vs_node_columns(model: models.DbtModel, node: models.DbtCatalogNode):
    model_columns = set(model.columns.keys())  # as defined in YML config
    catalogued_columns = set(node.columns.keys())  # as defined in SQL

    # if the YML and SQL columns exactly match, return early
    if not model_columns.symmetric_difference(catalogued_columns):
        return

    if model_columns.issubset(catalogued_columns):
        for undocumented_column in sorted(catalogued_columns.difference(model_columns)):
            logging.warning(
                f'Column {model.unique_id}.{undocumented_column} has not been documented in YML, '
                'but is present in the catalog. You should add it to your YML config, '
                'or (if it is not required) remove it from the model SQL file, run the model, '
                'and run `dbt docs generate` again')
        # after warning the user, return early
        return
    
    # otherwise, there are columns defined in YML that don't match what's defined in SQL
    for missing_column in sorted(model_columns.difference(catalogued_columns)):
        logging.warning(
            f'Column {model.unique_id}.{missing_column} documented in YML, '
            'but is not defined in the DBT catalog. Check the model SQL file '
            'and ensure you have run the model and `dbt docs generate`')
    return  # final return explicitly included for clarity


def parse_typed_models(raw_manifest: dict, raw_catalog: dict, tag: Optional[str] = None):
    catalog_nodes = parse_catalog_nodes(raw_catalog)
    dbt_models = parse_models(raw_manifest, tag=tag)
    adapter_type = parse_adapter_type(raw_manifest)

    logging.debug('Parsed %d models from manifest.json', len(dbt_models))
    for model in dbt_models:
        logging.debug(
            'Model %s has %d columns with %d measures',
            model.name,
            len(model.columns),
            reduce(lambda acc, col: acc + len(col.meta.measures) + len(col.meta.measure) + len(col.meta.metrics) + len(col.meta.metric), model.columns.values(), 0)
        )

    # Check catalog for models
    for model in dbt_models:
        if model.unique_id not in catalog_nodes:
            logging.warning(
                f'Model {model.unique_id} not found in catalog. No looker view will be generated. '
                f'Check if model has materialized in {adapter_type} at {model.relation_name}')
        else:
            # we know that the model is included in the catalog - extract it
            corresponding_catalog_node = catalog_nodes[model.unique_id]
            # issue warnings if the catalog columns (defined via SQL) don't match what's documented in YML
            compare_model_vs_node_columns(model, corresponding_catalog_node)

    # Update dbt models with data types from catalog
    dbt_typed_models = [
        model.copy(update={'columns': {
            column.name: column.copy(update={
                'data_type': get_column_type_from_catalog(catalog_nodes, model.unique_id, column.name)
            })
            for column in model.columns.values()
        }})
        for model in dbt_models
        if model.unique_id in catalog_nodes
    ]
    logging.debug('Found catalog entries for %d models', len(dbt_typed_models))
    logging.debug('Catalog entries missing for %d models', len(dbt_models) - len(dbt_typed_models))
    check_models_for_missing_column_types(dbt_typed_models)
    return dbt_typed_models


class ColumnNotInCatalogError(Exception):
    def __init__(self, model_id: str, column_name: str):
        super().__init__(
            f'Column {column_name} not found in catalog for model {model_id}, '
            'cannot find a data type for Looker. Is the column selected in the model SQL file, '
            'and have you run the model since adding the column to it?')


def get_column_type_from_catalog(catalog_nodes: Dict[str, models.DbtCatalogNode], model_id: str, column_name: str):
    node = catalog_nodes.get(model_id)
    column = None if node is None else node.columns.get(column_name)
    if column:
        return column.type
    # otherwise this will fail later when we try to map the data type to a Looker type
    raise ColumnNotInCatalogError(model_id, column_name)
