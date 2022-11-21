"""Entry point for histdatacom api

histdatacom(options)

Returns:
    data: returns a data frame or a list of data frames and metadata
"""
import sys
from pyarrow import Table
from datatable import Frame
from pandas import DataFrame
from histdatacom.options import Options
from . import histdata_com

__version__ = "0.77.01"
__author__ = 'David Midlo'

class APICaller(sys.modules[__name__].__class__): # type: ignore
    """APICaller. A Masquerade class.

    A class that extends sys.modules[__name__].__class__ (or the histdatacom class)
    extends/overwrites with a __call__ method to allow the module to be callable.

    Returns:
        data: returns a data frame or a list of data frames and metadata
    """
    def __call__(self, options: Options) -> list | Frame | DataFrame | Table:
        return histdata_com.main(options)

sys.modules[__name__].__class__ = APICaller
