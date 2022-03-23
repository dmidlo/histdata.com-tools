import random, requests, json, os

import defs

def get_random_seed() -> int:
    url = "https://api.random.org/json-rpc/4/invoke"
    id = random.randint(1,int(1e9))

    payload = {
        "jsonrpc": "2.0",
        "method": "generateIntegers",
        "params": {
            "apiKey": defs.RANDOM_ORG_KEY,
            "n": 1,
            "min": 1,
            "max": 1e9
        },
        "id": id
    }

    try:
        _random = json.loads(requests.post(url, json=payload).text)['result']['random']['data'][0]
    except:
        _random = id
    
    return _random

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
