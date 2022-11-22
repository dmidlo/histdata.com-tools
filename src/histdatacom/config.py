from histdatacom.records import Records
from multiprocessing import Queue
from typing import cast

args: dict = {}

available_remote_data: dict = dict()
repo_data_file_exists = False
filter_pairs = None

queue_manager = None

current_queue = None
next_queue = None
influx_chunks_queue = None

