import json
import os

import requests


class BCCollector:
    """Collects and saves JSON data from a specified URL.

    Attributes:
        url (str): The URL of the JSON data to collect.
    save_path (str): The path to the local file where the JSON data will be saved.

    Methods:
        collect() - Fetches JSON data from the URL and saves it to the specified file.

    Raises:
        requests.exceptions.HTTPError: If an HTTP error occurs during the download.
    Exception: If any other error occurs during the collection process.
    """

    def __init__(self, url, save_path):
        self.url = url
        self.save_path = save_path

    def collect(self):
        try:
            # Faz a solicitação HTTP
            response = requests.get(self.url)
            response.raise_for_status()  # Levanta um erro se o download falhar

            # Extrai o conteúdo JSON da resposta
            data = response.json()
            diretory = os.path.dirname(self.save_path)

            if diretory and not os.path.exists(self.save_path):
                os.makedirs(diretory)

            # Salva os dados JSON em um arquivo local
            with open(self.save_path, "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

            print(f"Dados salvos com sucesso em {self.save_path}")
        except requests.exceptions.HTTPError as http_err:
            print(f"Erro HTTP ocorreu: {http_err}")
        except Exception as err:
            print(f"Ocorreu um erro: {err}")
