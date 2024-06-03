import json
import os
import threading
from io import BytesIO
from zipfile import BadZipfile, ZipFile

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
        try:
            response = requests.head(zfiles_url)
            response.raise_for_status()

            with requests.get(zfiles_url) as download_response:
                download_response.raise_for_status()

                with ZipFile(BytesIO(requests.get(zfiles_url).content)) as files:
                    files_to_extract = set(files.namelist())

                    if files_to_extract.issubset(self.download_log_file):
                        print(
                            f"--> Files from {zfiles_url} already exists, skipping download."
                        )
                        return

                    files.extractall(self.save_path)

            for file in files_to_extract:
                print(f"--> File: {file} downloaded")
                self.download_log_file.add(file)

        except requests.RequestException as e:
            print(f"Error: downloading the file from {zfiles_url}: {e}")
        except BadZipfile:
            print(
                f"Error: The downloaded file from {zfiles_url} is not valid zip file."
            )
        except Exception as e:
            print(f"An unespected error ocurred while processing {zfiles_url}: {e}")

    def collect(self):
        """Collects all new daily information files from the CVM repository."""

        html = self._fetch()
        zfiles_list = html.xpath(self.xpath_query).getall()

        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)

        threads = []

        for zfile in zfiles_list:
            zfiles_url = os.path.join(self.url, zfile)
            thread = threading.Thread(
                target=self._download_and_extract_files, args=(zfiles_url,)
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        self._save_downloaded_logs()
        downloads_total = len(os.listdir(self.save_path))
        print(f"Total: {downloads_total} downloads.")


# ---


def main():
    save_path = "downloads"
    xpath_query = '//pre/a[contains(@href, "inf_diario_fi")]/@href'
    url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"

    cvm_datasets = CVMCollector(save_path, url, xpath_query)

    cvm_datasets.collect()


if __name__ == "__main__":
    main()
