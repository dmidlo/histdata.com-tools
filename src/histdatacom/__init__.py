import sys
from . import histdata_com

__version__ = "0.75.4"
__author__ = 'David Midlo'

class Options(sys.modules[__name__].__class__):
    def __call__(self, options):
        histdata_com.main(options)


sys.modules[__name__].__class__ = Options