from collections.abc import Callable, Iterator
from contextlib import contextmanager

from dbt.adapters.events.logging import AdapterLogger
from dbt.exceptions import ContractError
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtRuntimeError

import polars as pl
from dbt.adapters.polars.utils import polars_type_name, resolve_polars_type

logger = AdapterLogger("polars")


def contract_type_matches(declared_type: str, actual_type: pl.DataType) -> bool:
    if actual_type.base_type() != resolve_polars_type(declared_type):
        return False
    parameters = declared_type.partition("(")[2].rstrip(") ")
    if isinstance(actual_type, pl.Decimal) and parameters:
        precision, _, scale = parameters.partition(",")
        return actual_type.precision == int(precision) and actual_type.scale == int(
            scale.strip() or 0
        )
    return True


class ModelContract:
    """Enforces a model's contract on its result frame while it is written.

    The column names and types are checked against the schema. not_null and check
    constraints become filters that keep every row, but raise from inside the
    streaming write when a batch violates them, so nothing is committed.
    """

    def __init__(self, model: dict):
        contract = (model.get("config") or {}).get("contract") or {}
        self.enforced = bool(contract.get("enforced"))
        self.model_name = model.get("name", "")
        self.file_format = (model.get("config") or {}).get("file_format")
        self.columns: dict = model.get("columns") or {}
        self.model_constraints: list = model.get("constraints") or []
        self.violations: list[str] = []

    def apply(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        if not self.enforced:
            return frame
        self.check_columns(frame.collect_schema())
        frame = frame.select(list(self.columns))
        row_checks = list(self.row_checks())
        # A file is overwritten as it streams, so a write aborted by a guard would
        # leave the previous table truncated
        if row_checks and self.file_format not in (None, "delta"):
            raise DbtRuntimeError(
                f"Model '{self.model_name}' has not_null or check constraints, which "
                "dbt-polars only enforces on Delta and Iceberg tables, not with "
                f"file_format='{self.file_format}'."
            )
        for description, violation in row_checks:
            frame = frame.filter(
                violation.map_batches(
                    self.guard(description),
                    return_dtype=pl.Boolean,
                    is_elementwise=True,
                )
            )
        return frame

    def check_columns(self, schema: pl.Schema) -> None:
        yaml_columns = []
        sql_columns = []
        mismatch = len(schema) != len(self.columns)
        for name, column in self.columns.items():
            declared_type = column.get("data_type") or ""
            yaml_columns.append({"name": name, "data_type": declared_type})
            if name not in schema:
                mismatch = True
            elif not declared_type or not contract_type_matches(
                declared_type, schema[name]
            ):
                mismatch = True
        for name, actual_type in schema.items():
            declared_type = (self.columns.get(name) or {}).get("data_type") or ""
            matches = bool(declared_type) and contract_type_matches(
                declared_type, actual_type
            )
            # ContractError compares type strings, so an equivalent type is reported
            # with the declared name to keep it out of the mismatch table
            sql_columns.append(
                {
                    "name": name,
                    "data_type": declared_type
                    if matches
                    else polars_type_name(actual_type),
                }
            )
        if mismatch or not self.columns:
            raise ContractError(yaml_columns, sql_columns)

    def row_checks(self) -> Iterator[tuple[str, pl.Expr]]:
        for name, column in self.columns.items():
            for constraint in column.get("constraints") or []:
                yield from self.constraint_checks(constraint, [name])
        for constraint in self.model_constraints:
            yield from self.constraint_checks(
                constraint, constraint.get("columns") or []
            )

    def constraint_checks(
        self, constraint: dict, column_names: list[str]
    ) -> Iterator[tuple[str, pl.Expr]]:
        constraint_type = constraint.get("type")
        label = constraint.get("name") or constraint_type
        if constraint_type == ConstraintType.not_null:
            for name in column_names:
                yield f"not_null constraint on column '{name}'", pl.col(name).is_null()
        elif constraint_type == ConstraintType.check:
            expression = constraint.get("expression") or ""
            try:
                check = pl.sql_expr(expression)
            except Exception as exc:
                raise DbtRuntimeError(
                    f"Model '{self.model_name}' has a check constraint that dbt-polars "
                    f"can't evaluate: {expression!r}. {exc}"
                ) from exc
            yield f"{label} constraint ({expression})", (~check).fill_null(False)
        elif constraint_type == ConstraintType.custom:
            logger.warning(
                f"Model '{self.model_name}' has a custom constraint, which dbt-polars "
                "does not apply."
            )

    def guard(self, description: str) -> Callable[[pl.Series], pl.Series]:
        def raise_on_violation(violations: pl.Series) -> pl.Series:
            if violations.any():
                self.violations.append(description)
                raise DbtRuntimeError(
                    f"Model '{self.model_name}' violates its {description}."
                )
            return ~violations

        return raise_on_violation

    @contextmanager
    def violations_reported(self) -> Iterator[None]:
        """Re-raises a write that failed on a violation with a clear message.

        The guard's error can reach the caller wrapped, for example inside a
        DeltaError, so the recorded violation is raised instead.
        """
        try:
            yield
        except Exception as exc:
            if not self.violations:
                raise
            raise DbtRuntimeError(
                f"Model '{self.model_name}' violates its {self.violations[0]}."
            ) from exc
