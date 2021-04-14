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
    parser.validate_manifest(raw_manifest)    # FIX
    print(f'Detected valid manifest at {path}')
    return raw_manifest


def get_catalog(prefix: str):
    paths = pathlib.Path('./').rglob('catalog.json')
    try:
        path = next(paths)
    except StopIteration:
        raise SystemExit(f"No catalog.json file found in path {prefix}")
    with open(path, 'r') as f:
        raw_catalog = json.load(f)
    parser.validate_catalog(raw_catalog)    # FIX
    print(f'Detected valid catalog at {path}')
    return raw_catalog


def run():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        '--target',
        help='Path to dbt target directory containing manifest.json and catalog.json.',
        default='./'
    )
    argparser.add_argument(
        '--tag',
        help='Filter to dbt models using this tag'
    )
    args = argparser.parse_args()

    # Load raw manifest file
    raw_manifest = get_manifest(args.target)
    raw_catalog = get_catalog(args.target)

    # Get dbt models from manifest
    models = parser.parse_models(raw_manifest, tag=args.tag)
    catalog_nodes = parser.parse_catalog_nodes(raw_catalog)

    # Generate lookml views
    lookml_views = [
        generator.lookml_view_from_dbt_model(model, catalog_nodes)
        for model in models
    ]
    pathlib.Path(LOOKML_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    for view in lookml_views:
        with open(os.path.join(LOOKML_OUTPUT_DIR, view.filename), 'w') as f:
            f.write(view.contents)

    print(f'Generated {len(lookml_views)} lookml views in {LOOKML_OUTPUT_DIR}')

    # Generate Lookml model
    lookml_model = generator.lookml_model_from_dbt_project(models)
    with open(os.path.join(LOOKML_OUTPUT_DIR, lookml_model.filename), 'w') as f:
        f.write(lookml_model.contents)
    print(f'Generated 1 lookml model in {LOOKML_OUTPUT_DIR}')
