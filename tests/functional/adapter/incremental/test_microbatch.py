import pytest
from dbt.tests.util import (
    patch_microbatch_end_time,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)

from dbt.adapters.polars.testing.mixin import PolarsTestMixin
from dbt.adapters.polars.testing.utils import polars_read_relation

# Microbatch replaces a window in place, which dbt-polars only does on Delta and Iceberg
skip_file_formats = pytest.mark.skip_configs("csv", "parquet", "ndjson")


def input_model_sql(
    days: int, event_time_sql: str = "timestamp '2020-01-0{day} 10:00:00'"
):
    rows = " union all ".join(
        f"select {day} as id, {event_time_sql.format(day=day)} as event_time"
        for day in range(1, days + 1)
    )
    return "{{ config(materialized='table', event_time='event_time') }}\n" + rows


microbatch_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1),
) }}
select * from {{ ref('input_model') }}
"""

microbatch_python_model = """
def model(dbt, session):
    dbt.config(
        materialized="incremental",
        incremental_strategy="microbatch",
        event_time="event_time",
        batch_size="day",
        begin="2020-01-01",
    )
    return dbt.ref("input_model")
"""


def read_ids(project, relation_name="microbatch_model"):
    return [
        row[0]
        for row in polars_read_relation(
            project.adapter, relation_name, columns=["id"], order_by="id"
        )
    ]


@skip_file_formats
class MicrobatchRuns(PolarsTestMixin):
    microbatch_model_file: str
    microbatch_model: str

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql(3),
            self.microbatch_model_file: self.microbatch_model,
        }

    def test_microbatch_runs(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        assert read_ids(project) == [1, 2, 3]

        # Rerunning the same day rebuilds its batches without duplicating rows
        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run"])
        assert read_ids(project) == [1, 2, 3]

        write_file(input_model_sql(5), "models", "input_model.sql")
        run_dbt(["run", "--select", "input_model"])

        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        assert read_ids(project) == [1, 2, 3]

        with patch_microbatch_end_time("2020-01-04 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        assert read_ids(project) == [1, 2, 3, 4]

        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        assert read_ids(project) == [1, 2, 3, 4, 5]

        # A full refresh rebuilds every batch, and later batches don't overwrite
        # the first one
        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model", "--full-refresh"])
        assert read_ids(project) == [1, 2, 3, 4, 5]


class TestMicrobatchSql(MicrobatchRuns):
    microbatch_model_file = "microbatch_model.sql"
    microbatch_model = microbatch_model_sql


class TestMicrobatchPython(MicrobatchRuns):
    microbatch_model_file = "microbatch_model.py"
    microbatch_model = microbatch_python_model


@skip_file_formats
class TestMicrobatchDateEventTime(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql(
                3, "cast(timestamp '2020-01-0{day} 00:00:00' as date)"
            ),
            "microbatch_model.sql": microbatch_model_sql,
        }

    def test_date_event_time(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        assert read_ids(project) == [1, 2, 3]


tz_input_python_model = """
import polars as pl
from datetime import datetime

def model(dbt, session):
    dbt.config(materialized="table", event_time="event_time")
    # 00:30 in Brussels is 23:30 UTC on the previous day. Stored in UTC, the only
    # time zone Iceberg supports.
    local = [
        datetime(2020, 1, 2, 0, 30),
        datetime(2020, 1, 2, 12, 0),
        datetime(2020, 1, 3, 12, 0),
    ]
    return pl.DataFrame({
        "id": [1, 2, 3],
        "event_time": pl.Series(local)
        .dt.replace_time_zone("Europe/Brussels")
        .dt.convert_time_zone("UTC"),
    })
"""


@skip_file_formats
class TestMicrobatchTimeZoneEventTime(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.py": tz_input_python_model,
            "microbatch_model.sql": microbatch_model_sql,
        }

    def test_time_zone_event_time(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])
        assert read_ids(project) == [1, 2, 3]


out_of_window_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1),
) }}
select id, timestamp '2019-12-15 00:00:00' as event_time from {{ ref('input_model') }}
"""


@skip_file_formats
class TestMicrobatchOutOfWindowFails(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql(3),
            "microbatch_model.sql": microbatch_model_sql,
        }

    def test_out_of_window_row_fails_batch(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run"])

        write_file(out_of_window_model_sql, "models", "microbatch_model.sql")
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, log_output = run_dbt_and_capture(
                ["run", "--select", "microbatch_model"], expect_pass=False
            )

        assert (
            "violates its batch window [2020-01-02 00:00:00, 2020-01-03 00:00:00) "
            "on event_time 'event_time'" in log_output
        )
        assert read_ids(project) == [1, 2, 3]


@skip_file_formats
class TestMicrobatchRejectsUniqueKey(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql(1),
            "microbatch_model.sql": microbatch_model_sql.replace(
                "batch_size='day',", "batch_size='day', unique_key='id',"
            ),
        }

    def test_unique_key_rejected(self, project):
        with patch_microbatch_end_time("2020-01-01 13:57:00"):
            _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "sets unique_key" in log_output


@pytest.mark.require_profiles("local")
class TestMicrobatchRejectsFileFormat(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql(1),
            "microbatch_model.sql": microbatch_model_sql.replace(
                "batch_size='day',", "batch_size='day', file_format='parquet',"
            ),
        }

    def test_file_format_rejected(self, project):
        with patch_microbatch_end_time("2020-01-01 13:57:00"):
            _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "only supports on Delta and Iceberg" in log_output
