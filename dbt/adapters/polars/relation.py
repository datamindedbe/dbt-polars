from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dbt.adapters.base.relation import BaseRelation, EventTimeFilter
from dbt.adapters.contracts.relation import HasQuoting, RelationConfig

from dbt.adapters.polars.utils import naive_utc


@dataclass(frozen=True, eq=False, repr=False)
class PolarsRelation(BaseRelation):
    file_format: str | None = None
    read_options: dict = field(default_factory=dict)
    external_location: str | None = None

    @classmethod
    def create_from(
        cls: type[PolarsRelation],
        quoting: HasQuoting,
        relation_config: RelationConfig,
        **kwargs: Any,
    ) -> PolarsRelation:
        # This override allows using the catalog as an alias for the database
        # property on all relations

        config = relation_config.config
        catalog = (
            config.get("catalog") if config else None
        ) or relation_config.database
        file_format = config.get("file_format") if config else None
        read_options = config.get("read_options", {}) if config else {}
        # Only sources have `external`; dbt's ExternalTable is falsy without a location
        external = getattr(relation_config, "external", None)
        external_location = external.location if external else None

        relation = super().create_from(
            quoting,
            relation_config,
            file_format=file_format,
            read_options=read_options,
            external_location=external_location,
            **kwargs,
        )

        if not relation.catalog and catalog:
            return relation.replace(catalog=catalog)
        return relation

    def _render_event_time_filtered(self, event_time_filter: EventTimeFilter) -> str:
        # Polars SQL can't compare every date or timestamp column with dbt's string
        # bounds, but it can compare any of them, cast, with a timestamp literal
        column = f"cast({event_time_filter.field_name} as timestamp)"
        bounds = []
        if event_time_filter.start:
            bounds.append(
                f"{column} >= timestamp '{naive_utc(event_time_filter.start)}'"
            )
        if event_time_filter.end:
            bounds.append(f"{column} < timestamp '{naive_utc(event_time_filter.end)}'")
        return " and ".join(bounds)
