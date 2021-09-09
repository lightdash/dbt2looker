import argparse
import json
import logging
import pathlib
import os
try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version

import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


from . import parser
from . import generator

MANIFEST_PATH = './manifest.json'
DEFAULT_LOOKML_OUTPUT_DIR = './lookml'


def get_manifest(prefix: str):
    manifest_path = os.path.join(prefix, 'manifest.json')
    try:
        with open(manifest_path, 'r') as f:
            raw_manifest = json.load(f)
    except FileNotFoundError as e:
        logging.error(f'Could not find manifest file at {manifest_path}. Use --target-dir to change the search path for the manifest.json file.')
        raise SystemExit('Failed')
    parser.validate_manifest(raw_manifest)
    logging.debug(f'Detected valid manifest at {manifest_path}')
    return raw_manifest


def get_catalog(prefix: str):
    catalog_path = os.path.join(prefix, 'catalog.json')
    try:
        with open(catalog_path, 'r') as f:
            raw_catalog = json.load(f)
    except FileNotFoundError as e:
        logging.error(f'Could not find catalog file at {catalog_path}. Use --target-dir to change the search path for the catalog.json file.')
        raise SystemExit('Failed')
    parser.validate_catalog(raw_catalog)
    logging.debug(f'Detected valid catalog at {catalog_path}')
    return raw_catalog


def get_dbt_project_config(prefix: str):
    project_path  = os.path.join(prefix, 'dbt_project.yml')
    try:
        with open(project_path, 'r') as f:
            project_config = yaml.load(f, Loader=Loader)
    except FileNotFoundError as e:
        logging.error(f'Could a dbt_project.yml file at {project_path}. Use --project-dir to change the search path for the dbt_project.yml file.')
        raise SystemExit('Failed')
    logging.debug(f'Detected valid dbt config at {project_path}')
    return project_config


def run():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        '--version',
        action='version',
        version=f'dbt2looker {version("dbt2looker")}',
    )
    argparser.add_argument(
        '--project-dir',
        help='Path to dbt project directory containing dbt_project.yml. Default is "."',
        default='./',
        type=str,
    )
    argparser.add_argument(
        '--target-dir',
        help='Path to dbt target directory containing manifest.json and catalog.json. Default is "./target"',
        default='./target',
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
        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
        type=str,
        default='INFO',
    )
    argparser.add_argument(
        '--output-dir',
        help='Path to a directory that will contain the generated lookml files',
        default=DEFAULT_LOOKML_OUTPUT_DIR,
        type=str,
    )
    args = argparser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(levelname)-6s %(message)s',
        datefmt='%H:%M:%S',
    )

    # Load raw manifest file
    raw_manifest = get_manifest(prefix=args.target_dir)
    raw_catalog = get_catalog(prefix=args.target_dir)
    raw_config = get_dbt_project_config(prefix=args.project_dir)

    # Get dbt models from manifest
    dbt_project_config = parser.parse_dbt_project_config(raw_config)
    typed_dbt_models = parser.parse_typed_models(raw_manifest, raw_catalog, tag=args.tag)
    adapter_type = parser.parse_adapter_type(raw_manifest)

    # Generate lookml views
    lookml_views = [
        generator.lookml_view_from_dbt_model(model, adapter_type)
        for model in typed_dbt_models
    ]
    pathlib.Path(os.path.join(args.output_dir, 'views')).mkdir(parents=True, exist_ok=True)
    for view in lookml_views:
        with open(os.path.join(args.output_dir, 'views', view.filename), 'w') as f:
            f.write(view.contents)

    logging.info(f'Generated {len(lookml_views)} lookml views in {os.path.join(args.output_dir, "views")}')

    # Generate Lookml models
    lookml_models = [
        generator.lookml_model_from_dbt_model(model, dbt_project_config.name)
        for model in typed_dbt_models
    ]
    for model in lookml_models:
        with open(os.path.join(args.output_dir, model.filename), 'w') as f:
            f.write(model.contents)
    logging.info(f'Generated {len(lookml_models)} lookml models in {args.output_dir}')
    logging.info('Success')
