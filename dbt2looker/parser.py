import logging
import json
import jsonschema
import importlib.resources
from typing import Dict, Optional, List
from functools import reduce

from . import models


def validate_manifest(raw_manifest: dict):
    with importlib.resources.open_text("dbt2looker.dbt_json_schemas", "manifest_dbt2looker.json") as f:
        schema = json.load(f)
    v = jsonschema.Draft7Validator(schema)
    hasError = False
    for error in v.iter_errors(raw_manifest):
        raise_error_context(error)
        hasError = True
    if hasError:
        raise ValueError("Failed to parse dbt manifest.json")
    return True


def raise_error_context(error: jsonschema.ValidationError, offset=''):
    for error in sorted(error.context, key=lambda e: e.schema_path):
        raise_error_context(error, offset=offset + '  ')
    path = '.'.join([str(p) for p in error.absolute_path])
    logging.error(f'{offset}Error in manifest at {path}: {error.message}')


def validate_catalog(raw_catalog: dict):
    return True


def parse_dbt_project_config(raw_config: dict):
    return models.DbtProjectConfig(**raw_config)


def parse_catalog_nodes(raw_catalog: dict):
    catalog = models.DbtCatalog(**raw_catalog)
    return catalog.nodes


def parse_adapter_type(raw_manifest: dict):
    manifest = models.DbtManifest(**raw_manifest)
    return manifest.metadata.adapter_type


def parse_models(raw_manifest: dict, tag=None) -> List[models.DbtModel]:
    manifest = models.DbtManifest(**raw_manifest)
    all_models: List[models.DbtModel] = [
        node
        for node in manifest.nodes.values()
        if node.resource_type == 'model'
    ]
    filtered_models = (
        all_models if tag is None else [
            model for model in all_models
            if tag in model.tags
        ]
    )
    return filtered_models


def check_models_for_missing_column_types(dbt_typed_models: List[models.DbtModel]):
    for model in dbt_typed_models:
        if all([col.data_type is None for col in model.columns.values()]):
            logging.debug('Model %s has no typed columns, no dimensions will be generated. %s', model.unique_id, model)


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
                f'Check if model has materialized in {adapter_type} at {model.database}.{model.db_schema}.{model.name}')

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


def get_column_type_from_catalog(catalog_nodes: Dict[str, models.DbtCatalogNode], model_id: str, column_name: str):
    node = catalog_nodes.get(model_id)
    column = None if node is None else node.columns.get(column_name)
    return None if column is None else column.type
