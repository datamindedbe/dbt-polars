import polars as pl
import pytest
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable, write_deltalake
from polars.testing import assert_frame_equal

from dbt.adapters.polars.catalogs.formats.delta import DeltaFormat


@pytest.fixture
def table(tmp_path):
    path = str(tmp_path / "t")
    pl.DataFrame({"id": [1, 2], "value": ["a", "b"]}).write_delta(path)
    return path


def merge(path, df):
    DeltaFormat.merge(path, df, ["id"], None, None, False)


def last_operation(path):
    return list(DeltaTable(path).history(1))[0]["operation"]


def test_all_new_rows_are_appended(table):
    merge(table, pl.DataFrame({"id": [3, 4], "value": ["c", "d"]}))

    assert_frame_equal(
        pl.read_delta(table).sort("id"),
        pl.DataFrame({"id": [1, 2, 3, 4], "value": ["a", "b", "c", "d"]}),
    )
    assert last_operation(table) == "WRITE"


def test_a_matching_key_is_merged(table):
    merge(table, pl.DataFrame({"id": [2, 3], "value": ["B", "c"]}))

    assert_frame_equal(
        pl.read_delta(table).sort("id"),
        pl.DataFrame({"id": [1, 2, 3], "value": ["a", "B", "c"]}),
    )
    assert last_operation(table) == "MERGE"


def test_append_refused_after_a_concurrent_commit(table):
    dt = DeltaTable(table)
    write_deltalake(table, pl.DataFrame({"id": [3], "value": ["c"]}), mode="append")

    with pytest.raises(DbtRuntimeError, match="changed since version"):
        DeltaFormat._append_if_all_new(
            dt, table, pl.DataFrame({"id": [3], "value": ["c"]}), ["id"], False, None
        )

    assert pl.read_delta(table)["id"].sort().to_list() == [1, 2, 3]
