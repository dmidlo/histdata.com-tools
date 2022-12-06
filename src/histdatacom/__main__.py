# pylint: disable=invalid-name
"""histdatacom.

Allows histdatacom to be run as a module with
as >>> python -m histdatacom
"""

from . import histdata_com  # noqa:WPS130


def main() -> None:  # noqa:DAR401
    """Run histdata_com.main and raise SystemExit on completion.

    Raises:
        SystemExit: Exit when finished.
    """
    raise SystemExit(histdata_com.main())


if __name__ == "__main__":
    main()
