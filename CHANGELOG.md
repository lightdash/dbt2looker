# Changelog

Recent and upcoming changes to dbt2looker

## Unreleased
### Added
- support ephemeral models (#57)

## 0.11.0
### Added
- support label and hidden fields (#49)
- support non-aggregate measures (#41)
- support bytes and bignumeric for bigquery (#75)
- support for custom connection name on the cli (#78)

### Changed
- updated dependencies (#74)

### Fixed
- Types maps for redshift (#76)

### Removed
- Strict manifest validation (#77)

## 0.10.0
### Added
Support for dbt 1.x

### Removed
Support for dbt <1.x

## 0.9.3
### Fixed
- Fix name of models and view to add lkml suffix

## 0.9.2
### Fixed
- Bug in spark adapter

## 0.9.1
### Fixed
- Fixed bug where dbt2looker would crash if a dbt project contained an empty model

### Changed
- When filtering models by tag, models that have no tag property will be ignored

## 0.9.0
### Added
- Support for spark adapter (@chaimt)

### Changed
- Updated with support for dbt2looker (@chaimt)
- Lookml views now populate their "sql_table_name" using the dbt relation name

## 0.8.2
### Changed
- Measures with missing descriptions fall back to coloumn descriptions. If there is no column description it falls back to "{measure_type} of {column_name}".

## 0.8.1
### Added
- Dimensions have an `enabled` flag that can be used to switch off generated dimensions for certain columns with `enabled: false`
- Measures have been aliased with the following: `measures,measure,metrics,metric`

### Changed
- Updated dependencies

## 0.8.0
### Changed
- Command line interface changed argument from `--target` to `--target-dir`

### Added
- Added the `--project-dir` flag to the command line interface to change the search directory for `dbt_project.yml`
