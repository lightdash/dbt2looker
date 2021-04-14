from . import models


def validate_manifest(raw_manifest: dict):
    return True


def validate_catalog(raw_catalog: dict):
    return True


def parse_adapter_type(raw_manifest: dict):
    manifest = models.DbtManifest(**raw_manifest)
    return manifest.metadata.adapter_type


def parse_models(raw_manifest: dict, tag=None):
    manifest = models.DbtManifest(**raw_manifest)
    all_models = [
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


def parse_catalog_nodes(raw_catalog: dict):
    catalog = models.DbtCatalog(**raw_catalog)
    return catalog.nodes
