"""Allow offline registry inspection without the general host CLI."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
