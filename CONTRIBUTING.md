## Exposing a new lookml configuration in dbt2looker

If you'd like to expose some new lookml config (e.g. a dimension or measure field) you can follow the pattern in this commit: https://github.com/lightdash/dbt2looker/commit/b9e799969aad2930efd1bbc3d01b5a2a77db9d60

In general:
* Update `models.py` with the new `schema.yml` fields you'd like to expose
* Map new fields to lookml in `generator.py`
* Update the `/examples` directory with an example of your feature in the dbt `pages.yml` and the `pages.view` output
