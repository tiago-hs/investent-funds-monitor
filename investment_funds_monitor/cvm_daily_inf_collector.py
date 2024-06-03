import json
import os
import threading
from io import BytesIO
from zipfile import ZipFile

import requests
from parsel import Selector


class CVMCollector:
    """Collects and extracts daily information files from the CVM repository.

    Attributes:
        URL (str): Base URL for downloading daily information files.
        DOWNLOAD_LOG_FILES (str): Path to the log file that tracks downloaded files.
        path (str): Directory path where the extracted files will be saved.
        download_log_file (set): Set of filenames that have already been downloaded.
    """

    # URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
    DOWNLOAD_LOG_FILES = "download_log_file.json"

    def __init__(self, save_path, url, xpath_query):
        """Initializes the CVMDailyInfCollector with a specified path.

        Args:
            path (str): Directory path where the extracted files will be saved.
        """

        self.save_path = save_path
        self.url = url
        self.xpath_query = xpath_query
        self.download_log_file = self._log_downloaded_files()

    def _fetch(self):
        """Fetches the HTML content from the CVM URL.

        Returns:
            Selector: A Selector object containing the HTML content.
        """

        response = requests.get(self.url)
        html = Selector(text=response.text)
        return html

    def _log_downloaded_files(self):
        """Loads the log of downloaded files from a JSON file.

        Returns:
            set: A set of filenames that have already been downloaded.
        """

        if os.path.exists(self.DOWNLOAD_LOG_FILES):
            with open(self.DOWNLOAD_LOG_FILES, "r") as f:
                return set(json.load(f))
        return set()

    def _save_downloaded_logs(self):
        """Saves the log of downloaded files to a JSON file."""

        with open(self.DOWNLOAD_LOG_FILES, "w") as f:
            json.dump(list(self.download_log_file), f)

    def _download_and_extract_files(self, zfiles_url):
        """Downloads a zip file from the specified URL and extracts its contents.

        Args:
            zip_url (str): The URL of the zip file to download.
        """
        response = requests.get(zfiles_url)
        with ZipFile(BytesIO(response.content)) as zfiles:
            zfiles.extractall(self.save_path)
            zfiles_list = zfiles.namelist()  # Get the list name of the files
        for zfile in zfiles_list:
            breakpoint()
            print(f"--> File: {zfile} downloaded")
            self.download_log_file.add(os.path.basename(zfile))

    def collect(self):
        """Collects and extracts all new daily information files from the CVM repository."""

        html = self._fetch()
        # files_list = html.xpath('//pre/a[re:test(@href, "inf_diario")]/@href').getall()
        zfiles_list = html.xpath(self.xpath_query).getall()

        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)

        threads = []

        for zfile in zfiles_list:
            if zfile not in self.download_log_file:
                zfiles_url = os.path.join(self.url, zfile)
                # filename = os.path.join(self.save_path, zfile)
                thread = threading.Thread(
                    target=self._download_and_extract_files, args=(zfiles_url,)
                )
                thread.start()
                threads.append(thread)
            else:
                print(f"--> File: {zfile} already exists, skipping download.")

        for thread in threads:
            thread.join()

        self._save_downloaded_logs()
        downloads_total = len(os.listdir(self.save_path))
        print(f"Total: {downloads_total} downloads.")


# ---


def main():
    save_path = "downloads"
    xpath_query = '//pre/a[re:test(@href, "inf_diario")]/@href'
    url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"

    cvm_datasets = CVMCollector(save_path, url, xpath_query)

    cvm_datasets.collect()


if __name__ == "__main__":
    main()
