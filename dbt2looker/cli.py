import json
import pathlib
import os

from . import parser
from . import generator

MANIFEST_PATH = './manifest.json'
LOOKML_OUTPUT_DIR = './lookml'

def run():
    # Load raw manifest file
    with open(MANIFEST_PATH, 'r') as f:
        raw_manifest = json.load(f)

    # Validate manifest
    parser.validate(raw_manifest)

    # Get dbt models from manifest
    models = parser.parse_models(raw_manifest)
    print(models)

    # Generate lookml files
    lookml_views = [
        generator.lookml_view_from_dbt_model(model)
        for model in models
    ]

    # Write output
    pathlib.Path(LOOKML_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    for view in lookml_views:
        with open(os.path.join(LOOKML_OUTPUT_DIR, view.filename), 'w') as f:
            f.write(view.contents)
