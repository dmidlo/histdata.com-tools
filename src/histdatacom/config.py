"""Configure global attributes."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from multiprocessing.managers import SyncManager

    from histdatacom.records import Records

ARGS: dict = {}
REPO_DATA: dict = {}
REPO_DATA_FILE_EXISTS: bool = False
FILTER_PAIRS: set | None = None

QUEUE_MANAGER: SyncManager | None = None

CURRENT_QUEUE: Records | None = None
NEXT_QUEUE: Records | None = None
INFLUX_CHUNKS_QUEUE: None = None

REQUESTS_TIMEOUT: int = 10

POST_HEADERS: dict = {
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
