# dbt2looker

Use `dbt2looker` to generate Looker view files automatically from dbt models.

**Features**

* **Column descriptions** synced to looker
* **Dimension** for each column in dbt model
* **Dimension groups** for datetime/timestamp/date columns
* **Measures** defined through dbt column `metadata` [see below](#defining-measures)
* Looker types
* Warehouses: BigQuery, Snowflake, Redshift (postgres to come)

[![demo](https://raw.githubusercontent.com/hubble-data/dbt2looker/main/docs/demo.gif)](https://asciinema.org/a/407407)

## Quickstart

Run `dbt2looker` in the root of your dbt project *after compiling looker docs*.

**Generate Looker view files for all models:**
```shell
dbt docs generate
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

## Defining measures

You can define looker measures in your dbt `schema.yml` files. For example:

```yaml
models:
  - name: pages
    columns:
      - name: url
        description: "Page url"
      - name: event_id
        description: unique event id for page view
        meta:
          looker.com:  # looker config block for column
             measures:
               - name: Page views
                 type: count
```