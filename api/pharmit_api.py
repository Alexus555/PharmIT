import requests as requests
import json
import pandas as pd


class PharmitApiHook:

    def __init__(self, config) -> None:
        self._config = config
        self._file_id = None

    def get_file_id_by_date(self, period, date) -> None:
        url = f'{self._config["url"]}/loadhistories'

        headers = {
            'Content-Type': "application/json",
            'Authorization': f"Basic {self._config['token']}",
            'cache-control': "no-cache"
        }

        payload = {f"{period}": f"{date}"}

        r = requests.post(url, headers=headers, data=json.dumps(payload))

        print(f'Status code = {r.status_code}, text = {r.text}')

        if r.status_code == 200:
            self._file_id = r.text

    def upload_data(self, data) -> None:
        if self._file_id is None:
            print('No file_id exists')
            return

        total_count = data['Count']
        print(f'Total number of data = {total_count}')

        url = f'{self._config["url"]}/values'
        url_confirm = f'{self._config["url"]}/loadhistories/{self._file_id}_{total_count}'

        headers = {
            'Content-Type': "application/json",
            'Authorization': f"Basic {self._config['token']}",
            'cache-control': "no-cache"
        }

        total_pages = len(data['Pages'])
        for count, page in enumerate(data['Pages']):
            print(f'Page {count} from {total_pages}')

            page_data = page['Page']

            df = pd.DataFrame(page_data)
            df['FileId'] = self._file_id
            payload = df.to_json(orient='records', force_ascii=False).encode('utf-8')
            # payload = json.dumps(df.to_dict('records'))

            r = requests.post(url, headers=headers, data=payload)

            print(f'Status code = {r.status_code}, text = {r.text}')

        r = requests.put(url_confirm, headers=headers)

        print(f'Status code = {r.status_code}, text = {r.text}')

        if r.status_code == 200:
            if r.text == total_count:
                print('Successful upload')
            else:
                print(f'Sent {total_count} rows,  accepted {r.text} rows')
