from io import BytesIO
import json
import os
import threading
from zipfile import ZipFile

import requests
from parsel import Selector


class CVMDailyInfCollector:
    URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
    DOWNLOAD_LOG_FILES = "download_log_file.json"

    def __init__(self, path):
        self.path = path
        self.download_log_file = self._log_downloaded_files()

    def _fetch(self):
        response = requests.get(self.URL)
        html = Selector(text=response.text)
        return html

    def _log_downloaded_files(self):
        if os.path.exists(self.DOWNLOAD_LOG_FILES):
            with open(self.DOWNLOAD_LOG_FILES, "r") as f:
                return set(json.load(f))
        return set()

    def _save_downloaded_logs(self):
        with open(self.DOWNLOAD_LOG_FILES, "w") as f:
            json.dump(list(self.download_log_file), f)

    def _download_and_extract_files(self, zip_url, filename):
        zip_response = requests.get(zip_url)
        with ZipFile(BytesIO(zip_response.content)) as zfile:
            zfile.extractall(self.path)
        print(f"--> File: {filename} downloaded")
        self.download_log_file.add(os.path.basename(filename))

    def collect(self):
        html = self._fetch()
        files_list = html.xpath('//pre/a[re:test(@href, "inf_diario")]/@href').getall()

        if not os.path.exists(self.path):
            os.makedirs(self.path)

        threads = []

        for file in files_list:
            if file not in self.download_log_file:
                zip_url = os.path.join(self.URL, file)
                filename = os.path.join(self.path, file)
                thread = threading.Thread(
                    target=self._download_and_extract_files, args=(zip_url, filename)
                )
                thread.start()
                threads.append(thread)
            else:
                print(f"--> File: {file} already exists, skipping download.")

        # Aguardar todas as threads concluírem
        for thread in threads:
            thread.join()

        self._save_downloaded_logs()
        downloads_total = len(os.listdir(self.path))
        print(f"Total: {downloads_total} downloads.")


# ---


def main():
    path = "downloads"
    collector = CVMDailyInfCollector(path)
    collector.collect()


if __name__ == "__main__":
    main()
