import pytest
from dbt.tests.adapter.constraints.fixtures import (
    model_schema_yml,
    my_incremental_model_sql,
    my_model_incremental_with_nulls_sql,
    my_model_sql,
    my_model_with_nulls_sql,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseIncrementalConstraintsColumnsEqual,
    BaseTableConstraintsColumnsEqual,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file

from dbt.adapters.polars.testing.mixin import PolarsTestMixin
from dbt.adapters.polars.testing.utils import polars_read_relation


class PolarsColumnTypes:
    @pytest.fixture
    def string_type(self):
        return "text"

    @pytest.fixture
    def int_type(self):
        return "integer"

    @pytest.fixture
    def data_types(self):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", "integer", "Int32"],
            ["'1'", "text", "String"],
            ["true", "boolean", "Boolean"],
            ["timestamp '2013-11-03 00:00:00'", "timestamp", "Datetime"],
            ["cast(1.5 as double)", "double", "Float64"],
            ["cast('2013-11-03' as date)", "date", "Date"],
            ["cast(1.5 as decimal(10,2))", "decimal(10,2)", "Decimal"],
        ]


# not_null and check constraints are only enforced on Delta and Iceberg
skip_file_formats = pytest.mark.skip_configs("csv", "parquet", "ndjson")


@skip_file_formats
class TestTableConstraintsColumnsEqual(
    PolarsColumnTypes, PolarsTestMixin, BaseTableConstraintsColumnsEqual
):
    pass


@skip_file_formats
class TestIncrementalConstraintsColumnsEqual(
    PolarsColumnTypes, PolarsTestMixin, BaseIncrementalConstraintsColumnsEqual
):
    pass


@skip_file_formats
class ConstraintsRollback(PolarsTestMixin):
    model_sql: str
    null_model_sql: str

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": self.model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test_constraint_violation_keeps_previous_table(self, project):
        run_dbt(["run", "-s", "my_model"])

        write_file(self.null_model_sql, "models", "my_model.sql")
        results = run_dbt(["run", "-s", "my_model"], expect_pass=False)

        assert "violates its not_null constraint on column 'id'" in results[0].message
        rows = polars_read_relation(
            project.adapter, "my_model", columns=["id", "color", "date_day"]
        )
        assert rows == [(1, "blue", "2019-01-01")]


class TestTableConstraintsRollback(ConstraintsRollback):
    model_sql = my_model_sql
    null_model_sql = my_model_with_nulls_sql


class TestIncrementalConstraintsRollback(ConstraintsRollback):
    model_sql = my_incremental_model_sql
    null_model_sql = my_model_incremental_with_nulls_sql


valid_model_sql = "select cast(1 as bigint) as id, cast(5 as double) as amount"


def contract_schema_yml(model_name: str, model_constraints: str = "") -> str:
    return f"""
version: 2
models:
  - name: {model_name}
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: bigint
        constraints:
          - type: not_null
      - name: amount
        data_type: double
        constraints:
          - type: check
            expression: amount >= 0
{model_constraints}
"""


@pytest.mark.skip_profiles("iceberg")
class TestFileFormatRejectsEnforcedConstraints(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        model_sql = (
            "{{ config(file_format='parquet') }}\n"
            "select cast(1 as bigint) as id, cast(5 as double) as amount"
        )
        return {
            "my_model.sql": model_sql,
            "schema.yml": contract_schema_yml("my_model"),
        }

    def test_file_format_rejects_enforced_constraints(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "only enforces on Delta and Iceberg" in results[0].message


@pytest.mark.skip_profiles("iceberg")
class TestFileFormatContractWithoutConstraints(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        model_sql = (
            "{{ config(file_format='parquet') }}\n"
            "select cast(5 as double) as amount, cast(1 as bigint) as id"
        )
        schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: bigint
      - name: amount
        data_type: double
"""
        return {"my_model.sql": model_sql, "schema.yml": schema_yml}

    def test_file_format_contract_without_constraints(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "my_model", columns=["id", "amount"]
        )
        assert rows == [(1, 5.0)]


@skip_file_formats
class TestCheckConstraint(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": valid_model_sql.replace("5", "-5"),
            "schema.yml": contract_schema_yml("my_model"),
        }

    def test_check_constraint_violation_fails(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "check constraint (amount >= 0)" in results[0].message


@skip_file_formats
class TestModelLevelCheckConstraint(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        model_constraints = """    constraints:
      - type: check
        name: amount_below_id
        expression: amount < id
"""
        return {
            "my_model.sql": valid_model_sql,
            "schema.yml": contract_schema_yml("my_model", model_constraints),
        }

    def test_model_level_check_violation_fails(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "amount_below_id constraint (amount < id)" in results[0].message


filtered_nulls_model_sql = """
select id, amount from (
    select cast(1 as bigint) as id, cast(5 as double) as amount
    union all
    select cast(null as bigint) as id, cast(3 as double) as amount
) where id is not null
"""


@skip_file_formats
class TestConstraintChecksRowsAfterModelFilters(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": filtered_nulls_model_sql,
            "schema.yml": contract_schema_yml("my_model"),
        }

    def test_rows_removed_by_the_model_are_not_checked(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "my_model", columns=["id", "amount"]
        )
        assert rows == [(1, 5.0)]


@skip_file_formats
class TestUnenforcedConstraintWarns(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        model_constraints = """    constraints:
      - type: unique
        columns: [id]
"""
        return {
            "my_model.sql": valid_model_sql,
            "schema.yml": contract_schema_yml("my_model", model_constraints),
        }

    def test_unique_constraint_warns_and_runs(self, project):
        _, log_output = run_dbt_and_capture(["run"])
        assert "unique" in log_output
        assert "not enforced" in log_output


python_model_valid = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized="table")
    return pl.DataFrame({"amount": [2.5], "id": [1]})
"""

python_model_wrong_type = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized="table")
    return pl.DataFrame({"id": ["1"], "amount": [2.5]})
"""

python_model_with_nulls = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized="table")
    return pl.DataFrame({"id": [1, None], "amount": [2.5, 3.0]})
"""


@skip_file_formats
class TestPythonModelContract(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.py": python_model_valid,
            "schema.yml": contract_schema_yml("my_model"),
        }

    def test_python_model_contract(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "my_model", columns=["id", "amount"]
        )
        assert rows == [(1, 2.5)]

        write_file(python_model_wrong_type, "models", "my_model.py")
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "data type mismatch" in log_output

        write_file(python_model_with_nulls, "models", "my_model.py")
        results = run_dbt(["run"], expect_pass=False)
        assert "violates its not_null constraint on column 'id'" in results[0].message
        rows = polars_read_relation(
            project.adapter, "my_model", columns=["id", "amount"]
        )
        assert rows == [(1, 2.5)]
