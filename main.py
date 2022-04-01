import os
import shutil
import json
import config
from api.pharmit_api import PharmitApiHook


def save_to_json_file(file_name, data) -> None:
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_from_json_file(file_name) -> list:
    data = None
    with open(file_name, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data


def process_file(file_name) -> None:

    print(f'Processing file {file_name}')

    data = load_from_json_file(file_name)

    cfg = {'url': config.URL, 'token': config.TOKEN}

    pharm_api = PharmitApiHook(cfg)

    periods = {'DateSale', 'DecadeSale', 'MonthSale'}

    date = ''
    for period in periods:
        if period in data:
            date = data[period]
            break

    pharm_api.get_file_id_by_date(period, date)
    pharm_api.upload_data(data)


def get_list_of_files(directory) -> list:
    files = os.listdir(directory)

    json_files = list(filter(lambda x: x.endswith('.json') and x[:3] == '1c_', files))

    return json_files


if __name__ == '__main__':

    directory_to_process = config.DATA_DIRECTORY
    files_to_process = get_list_of_files(directory_to_process)

    for file_name in files_to_process:
        full_file_name = os.path.join(directory_to_process, file_name)
        process_file(full_file_name)
