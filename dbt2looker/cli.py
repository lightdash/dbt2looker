import argparse
import json
import logging
import pathlib
import os

import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


from . import parser
from . import generator

MANIFEST_PATH = './manifest.json'
LOOKML_OUTPUT_DIR = './lookml'


def get_manifest(prefix: str):
    paths = pathlib.Path(prefix).rglob('manifest.json')
    try:
        path = next(paths)
    except StopIteration as e:
        logging.error(f'No manifest.json file found in path {prefix}')
        raise SystemExit('Failed') from e
    with open(path, 'r') as f:
        raw_manifest = json.load(f)
    parser.validate_manifest(raw_manifest)    # FIX
    logging.debug(f'Detected valid manifest at {path}')
    return raw_manifest


def get_catalog(prefix: str):
    paths = pathlib.Path(prefix).rglob('catalog.json')
    try:
        path = next(paths)
    except StopIteration as e:
        logging.error(f'No catalog.json file found in path {prefix}')
        raise SystemExit('Failed') from e
    with open(path, 'r') as f:
        raw_catalog = json.load(f)
    parser.validate_catalog(raw_catalog)
    logging.debug(f'Detected valid catalog at {path}')
    return raw_catalog


def get_dbt_project_config(prefix: str):
    paths = pathlib.Path(prefix).rglob('dbt_project.yml')
    try:
        path = next(paths)
    except StopIteration as e:
        logging.error(f'No dbt_project.yml file found in path {prefix}')
        raise SystemExit('Failed') from e
    with open(path, 'r') as f:
        project_config = yaml.load(f, Loader=Loader)
    logging.debug(f'Detected valid dbt config at {path}')
    return project_config


def run():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        '--target',
        help='Path to dbt target directory containing manifest.json and catalog.json.',
        default='./',
        type=str,
    )
    argparser.add_argument(
        '--tag',
        help='Filter to dbt models using this tag',
        type=str,
    )
    argparser.add_argument(
        '--log-level',
        help='Set level of logs. Default is INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        type=lambda x: getattr(logging, x),
        default=logging.INFO,
    )
    args = argparser.parse_args()
    logging.basicConfig(
        level=args.log_level,
        format='%(asctime)s %(levelname)-6s %(message)s',
        datefmt='%H:%M:%S',
    )

    # Load raw manifest file
    raw_manifest = get_manifest(args.target)
    raw_catalog = get_catalog(args.target)
    raw_config = get_dbt_project_config(args.target)

    # Get dbt models from manifest
    dbt_project_config = parser.parse_dbt_project_config(raw_config)
    typed_dbt_models = parser.parse_typed_models(raw_manifest, raw_catalog, tag=args.tag)
    adapter_type = parser.parse_adapter_type(raw_manifest)

    # Generate lookml views
    lookml_views = [
        generator.lookml_view_from_dbt_model(model, adapter_type)
        for model in typed_dbt_models
    ]
    pathlib.Path(os.path.join(LOOKML_OUTPUT_DIR, 'views')).mkdir(parents=True, exist_ok=True)
    for view in lookml_views:
        with open(os.path.join(LOOKML_OUTPUT_DIR, 'views', view.filename), 'w') as f:
            f.write(view.contents)

    logging.info(f'Generated {len(lookml_views)} lookml views in {os.path.join(LOOKML_OUTPUT_DIR, "views")}')

    # Generate Lookml model
    lookml_model = generator.lookml_model_from_dbt_project(typed_dbt_models, dbt_project_name=dbt_project_config.name)
    with open(os.path.join(LOOKML_OUTPUT_DIR, lookml_model.filename), 'w') as f:
        f.write(lookml_model.contents)
    logging.info(f'Generated 1 lookml model in {LOOKML_OUTPUT_DIR}')
    logging.info('Success')
