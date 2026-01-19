# Hagstofan/api_client.py
import requests

class APIClient:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')

    def get(self, endpoint):
        url = f"{self.base_url}/{endpoint.strip('/')}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def post(self, endpoint, json_body):
        url = f"{self.base_url}/{endpoint.strip('/')}"
        response = requests.post(url, json=json_body)
        response.raise_for_status()
        return response.json()
