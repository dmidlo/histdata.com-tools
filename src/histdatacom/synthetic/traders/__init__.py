"""Public provider-neutral integration seams, not a production trader engine."""

from histdatacom.synthetic.bar_features import BarFeatureSourceV1
from histdatacom.synthetic.traders.adapters import (
    CatalogTraderDatasetReaderV1,
    CftcTraderPositioningReaderV1,
)
from histdatacom.synthetic.traders.inputs import (
    EconomicCalendarAsKnownReaderV1,
    TraderBarActivityReaderV1,
    TraderDatasetReaderV1,
    TraderPositioningReaderV1,
)
from histdatacom.synthetic.traders.triangle import (
    CommittedTraderTriangleReaderV1,
    TraderQuoteAvailabilityV1,
    TraderTriangleReaderV1,
    TraderTriangleRequestV1,
    TraderTriangleResultV1,
    TraderTriangleState,
)

__all__ = [
    "BarFeatureSourceV1",
    "CatalogTraderDatasetReaderV1",
    "CftcTraderPositioningReaderV1",
    "CommittedTraderTriangleReaderV1",
    "EconomicCalendarAsKnownReaderV1",
    "TraderBarActivityReaderV1",
    "TraderDatasetReaderV1",
    "TraderPositioningReaderV1",
    "TraderQuoteAvailabilityV1",
    "TraderTriangleReaderV1",
    "TraderTriangleRequestV1",
    "TraderTriangleResultV1",
    "TraderTriangleState",
]
