"""Configure process-wide constants and request defaults."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

REQUESTS_TIMEOUT: int = 10

POST_HEADERS: Mapping[str, str] = MappingProxyType(
    {
        "Host": "www.histdata.com",
        "Connection": "keep-alive",
        "Content-Length": "101",
        "Cache-Control": "max-age=0",
        "Origin": "http://www.histdata.com",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;"
            "q=0.8,application/signed-exchange;"
            "v=b3;"
            "q=0.9"
        ),
        "Referer": "",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
    }
)


def default_post_headers() -> dict[str, str]:
    """Return fresh default headers for a HistData archive POST request."""
    return dict(POST_HEADERS)
