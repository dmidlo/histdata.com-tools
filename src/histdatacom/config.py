from histdatacom.records import Records
from multiprocessing import Queue

args = None

available_remote_data = dict()
repo_data_file_exists = False

queue_manager = None

current_queue = None
next_queue = None
influx_chunks_queue = None

