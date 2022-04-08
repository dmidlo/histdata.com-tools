import os, yaml, sys

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

def load_influx_yaml():
    
    if os.path.exists('influxdb.yaml'):
        with open('influxdb.yaml', 'r') as file:
            try:
                yamlfile = yaml.safe_load(file)
            except yaml.YAMLError as exc:
                print(exc)
                pass

        return yamlfile
    else:
        print("\n ERROR: -I flag is used to import data to a influxdb instance...")
        print("\n        there is no influxdb.yaml file in working directory.")
        print("\n        did you forget to set it up?\n")
        sys.exit()