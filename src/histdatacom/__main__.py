"""histdatacom.

Allows histdatacom to be run as a module with
as >>> python -m histdatacom
"""

from . import histdata_com  # noqa:WPS130

raise SystemExit(histdata_com.main())

# pylint: skip-file
