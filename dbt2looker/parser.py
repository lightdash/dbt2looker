from . import models


def validate(raw_manifest: dict):
    return True


def parse_models(raw_manifest: dict, tag=None):
    manifest = models.DbtManifest(**raw_manifest)
    all_models = [node for node in manifest.nodes.values() if node.resource_type == 'model']
    return all_models if tag is None else [
        model for model in models
        if tag in model.tags
    ]
