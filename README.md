# dbt2looker

Generate lookml for views from dbt models

## Install

**PyPi repository**
Install from pypi into a fresh virtual environment. Requires python >=3.7
```
python3.7 -m venv dbt2looker-venv
source dbt2looker-venv/bin/activate
pip install dbt2looker
dbt2looker
```

**From source**
Install from source. Requires poetry and python >=3.7

```
poetry install
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

The lookml views will be saved in:
```
your_dbt_project_name/lookml
```
