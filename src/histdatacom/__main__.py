"""histdatacom
Allows histdatacom to be run as a module with
as >>> python -m histdatacom
"""
import sys
from . import histdata_com

sys.exit(histdata_com.main())
