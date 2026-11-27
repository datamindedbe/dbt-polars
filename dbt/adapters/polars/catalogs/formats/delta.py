import contextlib
import json
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from dbt_common.exceptions import DbtRuntimeError
from deltalake import CommitProperties, DeltaTable
from deltalake.exceptions import CommitFailedError
from deltalake.fs import DeltaStorageHandler
from deltalake.transaction import AddAction
from pyarrow.fs import PyFileSystem

import polars as pl
from dbt.adapters.polars.catalogs.baseCatalog import get_write_options
from dbt.adapters.polars.utils import first_day_from

# The size of a file an append writes, as Polars estimates the rows in memory.
APPEND_FILE_BYTES = 512 * 1024 * 1024
# The writer features an append of new files honors: the others need the writer to
# do more than add files (row tracking, column mapping, identity columns, ...).
APPEND_WRITER_FEATURES = {
    "appendOnly",
    "changeDataFeed",
    "checkConstraints",
    "checkpointProtection",
    "deletionVectors",
    "domainMetadata",
    "generatedColumns",
    "invariants",
    "timestampNtz",
    "v2Checkpoint",
    "vacuumProtocolCheck",
}
FIELD_RULES = ("delta.generationExpression", "delta.identity", "delta.invariants")


def _append_blocker(dt: DeltaTable) -> str | None:
    """Why new files cannot be committed to `dt` as they are written, or None."""
    metadata = dt.metadata()
    configuration = metadata.configuration
    protocol = dt.protocol()
    if metadata.partition_columns:
        return "a partitioned table"
    if (configuration.get("delta.columnMapping.mode") or "none") != "none":
        return "column mapping"
    if any(key.startswith("delta.constraints.") for key in configuration):
        return "CHECK constraints"
    if any(
        key.startswith(FIELD_RULES)
        for field in dt.schema().fields
        for key in field.metadata
    ):
        return "generated, identity or invariant columns"
    if protocol.min_writer_version > 7 or (
        set(protocol.writer_features or []) - APPEND_WRITER_FEATURES
    ):
        return "writer features"
    return None


def _stats_columns(dt: DeltaTable) -> set[str]:
    """The columns a file's statistics cover, as the table configures them."""
    configuration = dt.metadata().configuration
    names = [field.name for field in dt.schema().fields]
    if listed := configuration.get("delta.dataSkippingStatsColumns"):
        return {name.strip().strip("`") for name in listed.split(",")}
    count = int(configuration.get("delta.dataSkippingNumIndexedCols") or 32)
    return set(names if count < 0 else names[:count])


def _stat_value(value: Any, is_max: bool) -> Any:
    """A Parquet min/max as Delta writes it in a file's stats, or None to leave the
    column out. Timestamps have millisecond precision, a max rounded up."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, str)):
        return value
    if isinstance(value, datetime):
        aware = value.tzinfo is not None
        if aware:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        if is_max and value.microsecond % 1000:
            value += timedelta(microseconds=1000 - value.microsecond % 1000)
        millis = f"{value.microsecond // 1000:03d}"
        text = value.strftime("%Y-%m-%dT%H:%M:%S.") + millis
        return text + "Z" if aware else text
    if isinstance(value, date):
        return value.isoformat()
    return None


def _exact(statistics: Any) -> bool:
    return bool(
        getattr(statistics, "is_min_value_exact", False)
        and getattr(statistics, "is_max_value_exact", False)
    )


def _file_stats(metadata: pq.FileMetaData, columns: set[str]) -> str:
    """A data file's Delta stats from its Parquet footer: the row count, and the null
    count, min and max of top-level integer, string, date and timestamp columns.
    A bound that may be truncated is left out: a missing stat only loses pruning, a
    wrong one hides rows."""
    stats: dict[str, Any] = {
        "numRecords": metadata.num_rows,
        "minValues": {},
        "maxValues": {},
        "nullCount": {},
    }
    for j in range(metadata.num_columns if metadata.num_row_groups else 0):
        name = metadata.row_group(0).column(j).path_in_schema
        if name not in columns:
            continue
        chunks = [
            metadata.row_group(i).column(j) for i in range(metadata.num_row_groups)
        ]
        if any(c.statistics is None or not c.statistics.has_null_count for c in chunks):
            continue
        stats["nullCount"][name] = sum(c.statistics.null_count for c in chunks)
        bounded = [c.statistics for c in chunks if c.statistics.has_min_max]
        if not bounded or any(
            not c.statistics.has_min_max and c.statistics.null_count != c.num_values
            for c in chunks
        ):
            continue
        if isinstance(bounded[0].min, str) and not all(map(_exact, bounded)):
            continue
        low = _stat_value(min(s.min for s in bounded), is_max=False)
        high = _stat_value(max(s.max for s in bounded), is_max=True)
        if low is not None and high is not None:
            stats["minValues"][name] = low
            stats["maxValues"][name] = high
    return json.dumps(stats)


class DeltaFormat:
    @staticmethod
    def _write(
        path: str,
        data: pl.DataFrame | pl.LazyFrame,
        mode: str,
        model_config: dict,
        adapter_delta_opts: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        if (
            isinstance(data, pl.LazyFrame)
            and model_config.get("write_mode", "lazy") == "lazy"
        ):
            kwargs = get_write_options(
                pl.LazyFrame.sink_delta,
                model_config,
                ignore={"mode", "target", "storage_options"},
                merge={"delta_write_options": adapter_delta_opts},
            )
            data.sink_delta(path, mode=mode, storage_options=storage_options, **kwargs)  # type: ignore[call-overload]
        else:
            df = data.collect() if isinstance(data, pl.LazyFrame) else data
            kwargs = get_write_options(
                pl.DataFrame.write_delta,
                model_config,
                ignore={"mode", "target", "storage_options"},
                merge={"delta_write_options": adapter_delta_opts},
            )
            df.write_delta(path, mode=mode, storage_options=storage_options, **kwargs)  # type: ignore[call-overload]

    @staticmethod
    def write(
        path: str | Path,
        data: pl.DataFrame | pl.LazyFrame,
        mode: str,
        model_config: dict,
        partition_by: list[str] | None = None,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        adapter_delta_opts: dict = {
            "schema_mode": "overwrite" if mode == "overwrite" else "merge"
        }
        if partition_by:
            adapter_delta_opts["partition_by"] = partition_by
        DeltaFormat._write(
            str(path),
            data,
            mode,
            model_config,
            adapter_delta_opts,
            storage_options=storage_options,
        )

    @staticmethod
    def read(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> pl.LazyFrame:
        return pl.scan_delta(str(path), storage_options=storage_options)

    @staticmethod
    def append(
        path: str | Path,
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool,
        model_config: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        adapter_delta_opts = {"schema_mode": "merge"} if allow_schema_evolution else {}
        DeltaFormat._write(
            str(path),
            data,
            "append",
            model_config,
            adapter_delta_opts,
            storage_options=storage_options,
        )

    @staticmethod
    def replace_window(
        path: str | Path,
        data: pl.DataFrame | pl.LazyFrame,
        event_time: str,
        start: datetime,
        end: datetime,
        allow_schema_evolution: bool,
        model_config: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        """Overwrites only the rows whose event_time lies in `[start, end)`, in one
        commit. Delta rejects a row of `data` outside that range."""
        dtype = data.collect_schema()[event_time]
        if dtype == pl.Date:
            low, high = (first_day_from(moment).isoformat() for moment in (start, end))
        elif isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
            low, high = str(start), str(end)
        else:
            low, high = (
                moment.replace(tzinfo=timezone.utc).isoformat()
                for moment in (start, end)
            )
        column = '"' + event_time.replace('"', '""') + '"'
        adapter_delta_opts: dict = {
            "predicate": f"{column} >= '{low}' AND {column} < '{high}'"
        }
        if allow_schema_evolution:
            adapter_delta_opts["schema_mode"] = "merge"
        DeltaFormat._write(
            str(path),
            data,
            "overwrite",
            model_config,
            adapter_delta_opts,
            storage_options=storage_options,
        )

    @staticmethod
    def _append_if_all_new(
        dt: DeltaTable,
        path: str,
        data: pl.DataFrame | pl.LazyFrame,
        keys: list[str],
        storage_options: dict[str, str] | None,
    ) -> bool:
        """When no key of `data` is in the table, the merge only inserts: Polars writes
        the rows as new files, streaming, and delta-rs only commits them, instead of
        running delta-rs' merge, whose join holds the target in memory. The table is
        probed at the version `dt` is loaded at and the commit is pinned to it
        (max_commit_retries=0): a commit since then fails it. True if appended."""
        if _append_blocker(dt) is not None:
            return False
        version = dt.version()
        target = pl.scan_delta(path, version=version, storage_options=storage_options)
        schema = target.collect_schema()
        source = data.lazy()
        if set(source.collect_schema().names()) != set(schema.names()):
            return False
        source = source.select(schema.names()).cast(dict(schema))  # type: ignore[arg-type]

        # A target row can only match inside the source's key range, which lets the
        # scan skip files on their statistics. The range is one streaming pass.
        ranged = [
            k
            for k in keys
            if schema[k].is_integer()
            or schema[k].is_temporal()
            or schema[k] == pl.String
        ]
        extremes = source.select(
            pl.len().alias("rows"),
            *(pl.col(k).min().alias(f"min_{i}") for i, k in enumerate(ranged)),
            *(pl.col(k).max().alias(f"max_{i}") for i, k in enumerate(ranged)),
        ).collect(engine="streaming")
        if extremes.item(0, "rows") == 0:
            return True
        bounds = [
            pl.col(k).is_between(
                pl.lit(extremes.item(0, f"min_{i}"), dtype=schema[k]),
                pl.lit(extremes.item(0, f"max_{i}"), dtype=schema[k]),
            )
            for i, k in enumerate(ranged)
            if extremes.item(0, f"min_{i}") is not None
        ]
        candidates = target.select(keys)
        if bounds:
            candidates = candidates.filter(pl.all_horizontal(bounds))
        if candidates.limit(1).collect(engine="streaming").height:
            matched = source.select(keys).join(candidates, on=keys, how="semi")
            if matched.limit(1).collect(engine="streaming").height:
                return False

        prefix = f"dbt-polars-{uuid.uuid4()}"
        fs = PyFileSystem(DeltaStorageHandler(dt.table_uri, options=storage_options))
        sinked: list[Any] = []
        try:
            source.sink_parquet(
                pl.PartitionBy(
                    f"{path.rstrip('/')}/{prefix}",
                    approximate_bytes_per_file=APPEND_FILE_BYTES,
                ),
                storage_options=storage_options,
                mkdir=True,
                sinked_paths_callback=lambda args: sinked.extend(args.paths),
            )
            stats_columns = _stats_columns(dt)
            now = int(time.time() * 1000)
            actions = []
            for file in sinked:
                # The path relative to the table root, as the log records it.
                relative = prefix + file.path.replace("\\", "/").split(prefix, 1)[1]
                metadata = pq.read_metadata(fs.open_input_file(relative))
                if metadata.num_rows != file.num_rows:
                    raise DbtRuntimeError(
                        f"merge into {path}: {relative} has {metadata.num_rows} rows, "
                        f"the sink wrote {file.num_rows}"
                    )
                actions.append(
                    AddAction(
                        path=relative,
                        size=fs.get_file_info(relative).size,
                        partition_values={},
                        modification_time=now,
                        data_change=True,
                        stats=_file_stats(metadata, stats_columns),
                    )
                )
            dt.create_write_transaction(
                actions,  # type: ignore[arg-type]
                mode="append",
                schema=dt.schema(),
                commit_properties=CommitProperties(max_commit_retries=0),
            )
        except BaseException as e:
            with contextlib.suppress(Exception):
                fs.delete_dir(prefix)
            if isinstance(e, CommitFailedError):
                raise DbtRuntimeError(
                    f"merge into {path}: the table changed since version {version} "
                    "(a concurrent write committed); merge refused, run it again"
                ) from e
            raise
        return True

    @staticmethod
    def merge(
        path: str | Path,
        df: pl.DataFrame | pl.LazyFrame,
        keys: list[str],
        except_cols: list[str] | None,
        incremental_predicates: list[str] | None,
        allow_schema_evolution: bool,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        predicate = " AND ".join(
            f"DBT_INTERNAL_SOURCE.{k} = DBT_INTERNAL_DEST.{k}" for k in keys
        )
        if incremental_predicates:
            predicate += " AND " + " AND ".join(incremental_predicates)
        dt = DeltaTable(str(path), storage_options=storage_options)
        if not incremental_predicates and DeltaFormat._append_if_all_new(
            dt, str(path), df, keys, storage_options
        ):
            return
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        (
            dt.merge(
                df.to_arrow(),
                predicate,
                source_alias="DBT_INTERNAL_SOURCE",
                target_alias="DBT_INTERNAL_DEST",
                merge_schema=allow_schema_evolution,
            )
            .when_matched_update_all(except_cols=except_cols)
            .when_not_matched_insert_all()
            .execute()
        )

    @staticmethod
    def delete_matched(
        path: str | Path,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        predicate = " AND ".join(
            f"DBT_INTERNAL_SOURCE.{k} = DBT_INTERNAL_DEST.{k}" for k in keys
        )
        if incremental_predicates:
            predicate += " AND " + " AND ".join(incremental_predicates)
        dt = DeltaTable(str(path), storage_options=storage_options)
        (
            dt.merge(
                df.to_arrow(),
                predicate,
                source_alias="DBT_INTERNAL_SOURCE",
                target_alias="DBT_INTERNAL_DEST",
            )
            .when_matched_delete()
            .execute()
        )

    @staticmethod
    def truncate(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        DeltaTable(str(path), storage_options=storage_options).delete()

    @staticmethod
    def apply_snapshot(
        path: str | Path,
        rows_to_close: pl.DataFrame,
        rows_to_insert: pl.DataFrame,
        scd_id_col: str,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        staging = pl.concat([rows_to_close, rows_to_insert])
        dt = DeltaTable(str(path), storage_options=storage_options)
        (
            dt.merge(
                staging.to_arrow(),
                f"target.{scd_id_col} = source.{scd_id_col}",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    @staticmethod
    def get_partition_columns(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> list[str]:
        return (
            DeltaTable(str(path), storage_options=storage_options)
            .metadata()
            .partition_columns
        )

    @staticmethod
    def set_relation_comment(
        path: str | Path,
        comment: str,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        DeltaTable(
            str(path), storage_options=storage_options
        ).alter.set_table_description(comment)

    @staticmethod
    def get_relation_comment(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> str | None:
        return (
            DeltaTable(str(path), storage_options=storage_options)
            .metadata()
            .description
        )

    @staticmethod
    def set_column_comments(
        path: str | Path,
        comments: dict[str, str],
        storage_options: dict[str, str] | None = None,
    ) -> None:
        dt = DeltaTable(str(path), storage_options=storage_options)
        for column, comment in comments.items():
            dt.alter.set_column_metadata(column, {"comment": comment})

    @staticmethod
    def get_column_comments(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> dict[str, str]:
        dt = DeltaTable(str(path), storage_options=storage_options)
        return {
            field.name: field.metadata["comment"]
            for field in dt.schema().fields
            if field.metadata.get("comment")
        }
