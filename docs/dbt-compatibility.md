# dbt feature compatibility

Overview of features dbt-polars already supports and plans to support in the future.

| Status | Meaning |
|---|---|
| ✅ | Supported |
| 🟡 | Partially supported |
| ❌️ | Unsupported |

## dbt core features

| Feature | Status | Remarks |
|---|---|---|
| table materialization | ✅ | |
| incremental materialization | ✅ | ✅ append <br> ✅ merge <br> ✅ delete+insert<br/> ✅ microbatch |
| ephemeral materialization | ✅ | |
| view materialization | ❌️ | dbt-polars has no database to hold views. Use table or ephemeral instead. |
| Snapshots | ✅ | |
| Seeds | ✅ | |
| Sources | ✅ | |
| Python models | ✅ | |
| Data tests (generic and singular SQL) | ✅ | |
| Python singular tests | ✅ | |
| Unit tests (defined in YAML) | ✅ |  |
| persist_docs | ✅ | |
| dbt docs generate | ✅ | |
| dbt show | ✅ | |
| dbt clone | ❌️ | |
| Hooks | ❌️ | |
| Grants | ❌️ | |
| Model contracts | ✅ | |

## dbt core extensions

dbt-polars adds five features that are not supported in dbt-core:

- Ephemeral Python models
- Python data tests
- Unit tests on Python models
- Model contracts on Python models
- Microbatch on Python models

## Constraints

Support for the [constraints](https://docs.getdbt.com/reference/resource-properties/constraints) of a model contract:

| Constraint type | Definable | Enforced |
|---|---|---|
| not_null | ✅ | ✅ |
| primary_key | ✅ | ❌️ |
| foreign_key | ✅ | ❌️ |
| unique | ✅ | ❌️ |
| check | ✅ | ✅ |

A violation of an enforced constraint stops the write, and the previous version of the table stays in place. Incremental models only check the new rows.

Enforced constraints need a Delta or Iceberg table. A model with file_format parquet, csv or ndjson can have a contract, but no not_null or check constraints.

## Microbatch

Each batch of a [microbatch](https://docs.getdbt.com/docs/build/incremental-microbatch) model replaces the rows of its event_time window in a single commit. A batch fails when its result has a row with an event_time outside the window, and the table stays unchanged. To drop such rows instead, filter them in the model.

Microbatch needs a Delta or Iceberg table, and doesn't support unique_key or incremental_predicates.

## Macros

dbt-core publishes a list of  [cross-database macros](https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros) that each warehouse should implement.

| Macro group | Status | Remarks |
|---|---|---|
| Date/time | ✅ | |
| Hash | ❌️ |  |
| Type conversion | ✅ | |
| Strings | ✅ | |
| Arrays | 🟡 | Missing:<br>- array_append<br> - array_concat |
| Aggregates | 🟡 | Missing: listagg with an order or limit |
| Set operations | ✅ | |
| Other | ✅ | |

Unsupported macros raise a compilation error. Use a [Python model](guides/python-models.md) for the same logic.

## dbt packages

The table counts how many functions (macros and generic tests) of popular dbt packages work on dbt-polars.

| Package | Tested version | Supported | Partially supported | Not supported |
|---|---|---|---|---|
| [dbt_utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) | 1.4.1 | 39 | 0 | 5 |
| [codegen](https://hub.getdbt.com/dbt-labs/codegen/latest/) | 0.14.1 | 4 | 0 | 1 |
| [audit_helper](https://hub.getdbt.com/dbt-labs/audit_helper/latest/) | 0.11.0 | 5 | 1 | 1 |
| [dbt_expectations](https://hub.getdbt.com/metaplane/dbt_expectations/latest/) | 0.10.10 | 58 | 0 | 6 |

### dbt_utils

Not supported:

- `generate_surrogate_key` needs a hash function, which Polars SQL doesn't have.
- `get_relations_by_pattern` and `get_relations_by_prefix` query `information_schema`, which dbt-polars doesn't provide.
- `at_least_one` uses a `with` clause inside a subquery and `having` without `group by`. Polars SQL supports neither.
- `not_constant` uses `having` without `group by`. It works when you set `group_by_columns`.

### codegen

Not supported:

- `generate_source` lists tables through `information_schema`. It works when you pass `table_names`.

### audit_helper

Not supported:

- `compare_all_columns` uses a `with` clause inside a subquery, which Polars SQL doesn't support.

Partially supported:

- `compare_relation_columns` returns an empty `column_name` for columns that only exist in `b_relation`.

Primary keys made of several columns don't work in any macro, because combining them needs a hash function.

### dbt_expectations

The regex tests (`expect_column_values_to_match_regex`, `expect_column_values_to_not_match_regex` and their `_list` variants) need a dbt-polars version of a dbt_expectations macro. Add this to your `dbt_project.yml` to use it:

```yaml
dispatch:
  - macro_namespace: dbt_expectations
    search_order: ["dbt", "dbt_expectations"]
```

Not supported:

- `expect_column_median_to_be_between` and `expect_column_quantile_values_to_be_between` use `percentile_cont`, which Polars SQL doesn't have.
- `expect_row_values_to_have_recent_data` and `expect_grouped_row_values_to_have_recent_data` use dbt_date's current time, which needs `convert_timezone`.
- `expect_column_values_to_be_within_n_moving_stdevs` uses a `rows between` window frame, which Polars SQL doesn't support.
- `expect_select_column_values_to_be_unique_within_record` needs a hash function.
