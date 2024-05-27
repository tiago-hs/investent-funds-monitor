import json
import os
import threading
from io import BytesIO
from zipfile import ZipFile

import requests
from parsel import Selector


class CVMDailyInfCollector:
    """Collects and extracts daily information files from the CVM repository.

    Attributes:
        URL (str): Base URL for downloading daily information files.
        DOWNLOAD_LOG_FILES (str): Path to the log file that tracks downloaded files.
        path (str): Directory path where the extracted files will be saved.
        download_log_file (set): Set of filenames that have already been downloaded.
    """

    URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
    DOWNLOAD_LOG_FILES = "download_log_file.json"

    def __init__(self, path):
        """Initializes the CVMDailyInfCollector with a specified path.

        Args:
            path (str): Directory path where the extracted files will be saved.
        """

        self.path = path
        self.download_log_file = self._log_downloaded_files()

    def _fetch(self):
        """Fetches the HTML content from the CVM URL.

        Returns:
            Selector: A Selector object containing the HTML content.
        """

        response = requests.get(self.URL)
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

    def _download_and_extract_files(self, zip_url, filename):
        """Downloads a zip file from the specified URL and extracts its contents.

        Args:
            zip_url (str): The URL of the zip file to download.
            filename (str): The name of the file to save the zip content temporarily.
        """
        zip_response = requests.get(zip_url)
        with ZipFile(BytesIO(zip_response.content)) as zfile:
            zfile.extractall(self.path)
            extracted_files = zfile.namelist()  # Get the list name of the files
        for file in extracted_files:
            print(f"--> File: {file} downloaded")
            self.download_log_file.add(os.path.basename(file))

    def collect(self):
        """Collects and extracts all new daily information files from the CVM repository."""

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

        for thread in threads:
            thread.join()

        self._save_downloaded_logs()
        downloads_total = len(os.listdir(self.path))
        print(f"Total: {downloads_total} downloads.")
