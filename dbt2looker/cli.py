import argparse
import json
import pathlib
import os

from . import parser
from . import generator

MANIFEST_PATH = './manifest.json'
LOOKML_OUTPUT_DIR = './lookml'


def get_manifest(prefix: str):
    paths = pathlib.Path('./').rglob('manifest.json')
    try:
        path = next(paths)
    except StopIteration:
        raise SystemExit(f"No manifest.json file found in path {prefix}")
    with open(path, 'r') as f:
        raw_manifest = json.load(f)
    parser.validate(raw_manifest)
    print(f'Detected valid manifest at {path}')
    return raw_manifest


def run():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        '--manifest',
        help='Path to manifest.json file. Examples: ./target, manifest.json',
        default='./'
    )
    argparser.add_argument(
        '--tag',
        help='Filter to dbt models using this tag'
    )
    args = argparser.parse_args()

    # Load raw manifest file
    raw_manifest = get_manifest(args.manifest)

    # Get dbt models from manifest
    models = parser.parse_models(raw_manifest, tag=args.tag)

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
