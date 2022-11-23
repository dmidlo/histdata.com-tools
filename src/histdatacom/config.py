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

REQUESTS_TIMEOUT = 10
