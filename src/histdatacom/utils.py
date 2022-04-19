import os, yaml, sys, pytz
from datetime import datetime
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn

def get_month_from_datemonth(datemonth):
    return datemonth[-2:] if datemonth is not None and len(datemonth) > 4 else ""

def get_year_from_datemonth(datemonth):
    return datemonth[:4] if datemonth is not None else ""

def get_query_string(url):
    return url.split('?')[1].split('/')

def create_full_path(path_str):
    if not os.path.exists(path_str):
        os.makedirs(path_str)

def set_working_data_dir(data_dirname):
    return f"{os.getcwd()}{os.sep}{data_dirname}{os.sep}"

def load_influx_yaml():
    
    if os.path.exists('influxdb.yaml'):
        with open('influxdb.yaml', 'r') as file:
            try:
                yamlfile = yaml.safe_load(file)
            except yaml.YAMLError as exc:
                print(exc)
        return yamlfile
    else:
        print("\n ERROR: -I flag is used to import data to a influxdb instance...")
        print("\n        there is no influxdb.yaml file in working directory.")
        print("\n        did you forget to set it up?\n")
        sys.exit()

def get_current_datemonth_gmt_plus5():
    now = datetime.now().astimezone()
    gmt_plus5 = now.astimezone(pytz.timezone("Etc/GMT+5"))
    return f"{gmt_plus5.year}{gmt_plus5.strftime('%m')}"

def get_progress_bar(progress_string):
    
    return TextColumn(text_format=progress_string), \
            BarColumn(),\
            "[progress.percentage]{task.percentage:>3.0f}%", \
            TimeElapsedColumn()