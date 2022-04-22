import sys
from . import histdata_com

from rich import print


class Options(sys.modules[__name__].__class__):
    def __call__(self, options):
        sys.exit(histdata_com.main(options))
        return 0

sys.modules[__name__].__class__ = Options