# dbt2looker

**Requires python >=3.7**

Use `dbt2looker` to generate Looker view files automatically from dbt models.

### Usage

Run `dbt2looker` in the root of your dbt project:

**Generate Looker view files for all models:**
```shell
dbt2looker
```

**Generate Looker view files for all models tagged `prod`**
```shell
dbt2looker --tag prod
```

## Install

**Install from PyPi repository**

Install from pypi into a fresh virtual environment.

```
# Create virtual env
python3.7 -m venv dbt2looker-venv
source dbt2looker-venv/bin/activate

# Install
pip install dbt2looker

# Run
dbt2looker
```

**Build from source**

Requires [poetry](https://python-poetry.org/docs/) and python >=3.7

```
# Install
poetry install

# Run
poetry run dbt2looker
```

## Usage

#### Generate lookml views for all dbt models in a project.
Within your dbt project, run:
```
dbt2looker
```
The lookml views will be saved in:
```
your_dbt_project_name/lookml
```

#### Generate lookml views for a specific set of models.
This is basically the same as above, except you list the specific models you're interested in. Each model name should be separated by a space:
```
dbt2looker olivers_cool_model kts_cooler_model
```

The lookml views for `olivers_cool_model` and `kts_cooler_model` will be saved in:
```
your_dbt_project_name/lookml
```
