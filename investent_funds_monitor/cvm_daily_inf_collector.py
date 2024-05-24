import os
import threading

import requests
from parsel import Selector


class CVMDailyInfCollector:
    URL = 'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'

    def __init__(self, path):
        self.path = path

    def _fetch(self):
        response = requests.get(self.URL)
        html = Selector(text=response.text)
        return html

    def _download_file(self, zip_url, filename):
        with open(filename, 'wb') as f:
            zip_response = requests.get(zip_url)
            f.write(zip_response.content)
        print(f'--> File: {filename} downloaded.')

    def collect(self):
        html = self._fetch()
        files_list = html.xpath(
            '//pre/a[re:test(@href, "inf_diario")]/@href'
        ).getall()

        if not os.path.exists(self.path):
            os.makedirs(self.path)

        threads = []
        for file in files_list:
            zip_url = os.path.join(self.URL, file)
            filename = os.path.join(self.path, file)
            thread = threading.Thread(
                target=self._download_file, args=(zip_url, filename)
            )
            thread.start()
            threads.append(thread)

        # Aguardar todas as threads concluírem
        for thread in threads:
            thread.join()

        downloads_total = len(os.listdir(self.path))
        print(f'Total: {downloads_total} downloads.')


# ---


def main():
    path = 'downloads'
    collector = CVMDailyInfCollector(path)
    collector.collect()


if __name__ == '__main__':
    main()
