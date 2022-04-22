import sys
from . import histdata_com
from contextlib import contextmanager


class Options(sys.modules[__name__].__class__):
    def __call__(self, options):
        histdata_com.main(options)


sys.modules[__name__].__class__ = Options