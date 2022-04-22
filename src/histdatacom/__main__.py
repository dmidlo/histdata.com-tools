"""_summary_
Allows histdatacom to be run as a module with
as >>> python -m histdatacom
"""
import sys
from . import histdata_com


# if __name__ == '__main__':
sys.exit(histdata_com.main())
