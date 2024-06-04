import json
import os

import requests


class BCDataCollector:
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
