"""Narrow adapters over canonical positioning and dataset implementations."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar

from histdatacom.datasets.catalog import DatasetCatalog
from histdatacom.datasets.contracts import (
    MAX_CATALOG_ITEMS,
    DatasetQueryScopeV1,
    DatasetResolutionV1,
    DatasetVerificationV1,
    DatasetVersionManifestV1,
)
from histdatacom.market_context.positioning import (
    CftcPositioningCorpusV1,
    CftcPositioningQueryV1,
    CftcReportFamily,
    CftcReportScope,
    query_cftc_positioning_corpus,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.traders.inputs import bounded_clock

_Entry = TypeVar("_Entry")


def _merge_entries(
    left: tuple[_Entry, ...],
    right: tuple[_Entry, ...],
    key: Callable[[_Entry], str],
) -> tuple[_Entry, ...]:
    if max(len(left), len(right)) > MAX_CATALOG_ITEMS:
        raise ValueError("registration exceeds catalog item budget")
    entries = {key(item): item for item in left}
    for item in right:
        name = key(item)
        if name in entries and entries[name] != item:
            raise ValueError(
                "registration conflicts with retained catalog entry"
            )
        entries[name] = item
        if len(entries) > MAX_CATALOG_ITEMS:
            raise ValueError("registration exceeds catalog item budget")
    return tuple(entries.values())


@dataclass(frozen=True, slots=True)
class CftcTraderPositioningReaderV1:
    """Process-local strict adapter; acquisition/replay stay with the owner."""

    corpus: CftcPositioningCorpusV1

    def __post_init__(self) -> None:
        if not isinstance(self.corpus, CftcPositioningCorpusV1):
            raise TypeError("positioning requires the canonical corpus")

    def as_known_at(
        self,
        *,
        start_ns: int,
        end_ns: int,
        decision_at_ns: int,
        symbols: Sequence[str],
        report_families: Sequence[CftcReportFamily],
        report_scopes: Sequence[CftcReportScope],
        max_staleness_days: int | None = None,
    ) -> CftcPositioningQueryV1:
        """Never fall back from strict ex-ante to current corrected history."""
        for name, value in (
            ("start_ns", start_ns),
            ("end_ns", end_ns),
            ("decision_at_ns", decision_at_ns),
        ):
            bounded_clock(value, name)
        if not start_ns < end_ns or decision_at_ns > start_ns:
            raise ValueError("positioning decision must not follow its window")
        if not 1 <= len(symbols) <= 3:
            raise ValueError("positioning symbol request exceeds its bound")
        if (
            not 1 <= len(report_families) <= 2
            or not 1 <= len(report_scopes) <= 2
        ):
            raise ValueError("positioning family/scope request exceeds bounds")
        return query_cftc_positioning_corpus(
            self.corpus,
            start_ns=start_ns,
            end_ns=end_ns,
            as_of_ns=decision_at_ns,
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            symbols=symbols,
            report_families=report_families,
            report_scopes=report_scopes,
            max_staleness_days=max_staleness_days,
        )


@dataclass(frozen=True, slots=True)
class CatalogTraderDatasetReaderV1:
    """Immutable registration composition, without a second dataset registry.

    Registration validates the canonical catalog but does not create scientific
    qualification or read source data. Explicit verify() checks current bytes.
    """

    catalog: DatasetCatalog

    def __post_init__(self) -> None:
        if not isinstance(self.catalog, DatasetCatalog):
            raise TypeError("dataset reader requires the canonical catalog")

    def register(
        self, addition: DatasetCatalog
    ) -> CatalogTraderDatasetReaderV1:
        """Add a self-contained canonical registration; never update aliases."""
        if not isinstance(addition, DatasetCatalog):
            raise TypeError("registration requires a canonical catalog")

        return CatalogTraderDatasetReaderV1(
            DatasetCatalog(
                providers=_merge_entries(
                    self.catalog.providers,
                    addition.providers,
                    lambda item: item.source_provider_id,
                ),
                adapters=_merge_entries(
                    self.catalog.adapters,
                    addition.adapters,
                    lambda item: item.adapter_id,
                ),
                datasets=_merge_entries(
                    self.catalog.datasets,
                    addition.datasets,
                    lambda item: item.dataset_id,
                ),
                versions=_merge_entries(
                    self.catalog.versions,
                    addition.versions,
                    lambda item: item.dataset_version_id,
                ),
                aliases=_merge_entries(
                    self.catalog.aliases,
                    addition.aliases,
                    lambda item: item.alias,
                ),
            )
        )

    def resolve(
        self, reference: str, *, query_scope: DatasetQueryScopeV1
    ) -> DatasetResolutionV1:
        return self.catalog.resolve(reference, query_scope=query_scope)

    def replay(self, receipt: DatasetResolutionV1) -> DatasetResolutionV1:
        return self.catalog.replay(receipt)

    def verify(self, receipt: DatasetResolutionV1) -> DatasetVerificationV1:
        return self.catalog.verify(receipt)

    def lineage(
        self, receipt: DatasetResolutionV1
    ) -> tuple[DatasetVersionManifestV1, ...]:
        """Traverse native exact parent IDs; detect cycles rather than truncate."""
        self.catalog.replay(receipt)
        versions = {v.dataset_version_id: v for v in self.catalog.versions}
        finished: set[str] = set()
        active: set[str] = set()
        stack = [(receipt.dataset_version_id, False)]
        while stack:
            key, leaving = stack.pop()
            if leaving:
                active.remove(key)
                finished.add(key)
                continue
            if key in active:
                raise ValueError("dataset parent lineage contains a cycle")
            if key in finished:
                continue
            active.add(key)
            stack.append((key, True))
            stack.extend(
                (p.parent_dataset_version_id, False)
                for p in reversed(versions[key].parents)
            )
            if len(stack) > MAX_CATALOG_ITEMS * 2:
                raise ValueError("dataset parent closure exceeds work budget")
        return tuple(versions[key] for key in sorted(finished))
