"""Entry point for histdatacom api.

histdatacom(options)

Returns:
    data: returns a data frame or a list of data frames and metadata
"""
from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from histdatacom.fx_enums import Format, Pairs, Timeframe
from histdatacom.options import Options

from . import histdata_com  # noqa:WPS130

if TYPE_CHECKING:
    from datatable import Frame  # noqa:I900
    from pandas import DataFrame
    from pyarrow import Table

__all__ = [
    "Options",
    "Pairs",
    "Timeframe",
    "Format",
]


__version__ = "0.77.04"
__author__ = "David Midlo"


class APICaller(sys.modules[__name__].__class__):  # type: ignore # noqa:H601
    """APICaller. A Masquerade class.

    A class that extends sys.modules[__name__].__class__ (the histdatacom class)
    extends/overwrites with a __call__ method to allow the module to be callable.

    Returns:
        data: returns a data frame or a list of data frames and metadata
    """

    def __call__(  # noqa:BLK001
        self, options: Options
    ) -> "list" | "Frame" | "DataFrame" | "Table":
        """Run histdatacom -h for help.

        Args:
            options (Options): a histdatacom Options object.

        Returns:
            "list" | "Frame" | "DataFrame" | "Table":
                - (list) if called with -A or -U
                - (Frame) if options.api_return_type = "datatable"
                - (DataFrame) if options.api_return_type = "pandas"
                - (Table) if options.api_return_type = "arrow"
        """
        return histdata_com.main(options)


sys.modules[__name__].__class__ = APICaller
