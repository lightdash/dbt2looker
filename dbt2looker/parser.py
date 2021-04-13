from . import models


def validate(raw_manifest: dict):
    return True

def parse_models(raw_manifest: dict):
    manifest = models.DBTManifest(raw_manifest)
    return [node for node in manifest.nodes if node.resource_type == 'model']
