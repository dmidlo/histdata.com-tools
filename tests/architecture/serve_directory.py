"""Simple Flask App to serve the contents of the current directory.

$ python serve_directory.py

this serves browseable contents of this file's directory.
to http://localhost:8080.

"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from flask import Flask, send_from_directory

if TYPE_CHECKING:
    from typing import Iterator

    from flask import Response

# Instantiate a Flask app object
app: Flask = Flask(__name__)

# Get the parent directory of this script. (Global)
DIR_PATH: Path = Path(__file__).parent


def get_files_from_this_directory() -> Iterator[str]:
    """Generate the items within this script's directory.

    Yields:
        Generator: item(s) in __file__'s directory.
    """
    for dir_item in DIR_PATH.iterdir():
        yield dir_item.name


@app.route("/files/<file_name>")  # type: ignore
def serve_file(file_name: str) -> Response:
    """Set up a dynamic routes for directory items at /files/.

    Args:
        file_name (str): regular file.

    Returns:
        Response: regular file.
    """
    return send_from_directory(DIR_PATH, file_name)


def html_ul_of_items() -> str:
    """Create a unordered list of anchors/links to file routes.

    Returns:
        str: a <ul> with N <li> elements where N is the number of
            elements in __file__'s directory.
    """
    html: str = "<ul>"
    for dir_item in get_files_from_this_directory():
        html += f"<li><a href='files/{dir_item}'>{dir_item}</a`></li>"
    return f"{html}</ul>"


@app.route("/")  # type: ignore
def serve_index() -> str:
    """Root route which displays an unordered list of directory items.

    Returns:
        str: a <ul> with N <li> elements where N is the number of
            elements in __file__'s directory.
    """
    return html_ul_of_items()


def main() -> None:
    """Run the flask app."""
    app.run(port=8080)


if __name__ == "__main__":
    main()
