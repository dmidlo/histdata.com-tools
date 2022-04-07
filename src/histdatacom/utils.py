import os

def get_month_from_datemonth(datemonth):
    if len(datemonth) > 4:
        return datemonth[-2:]
    else:
        return ""

def get_year_from_datemonth(datemonth):
    return datemonth[:4]

def get_query_string(url):
    return url.split('?')[1].split('/')

def create_full_path(path_str):
    if not os.path.exists(path_str):
        os.makedirs(path_str)

def set_working_data_dir(data_dirname):
    return os.getcwd() + os.sep + data_dirname + os.sep
