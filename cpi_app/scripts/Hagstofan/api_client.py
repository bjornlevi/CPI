# Hagstofan/api_client.py
import requests

class APIClient:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')

    def _url(self, endpoint):
        return f"{self.base_url}/{endpoint.strip('/')}"

    def _alternate_endpoint(self, endpoint):
        endpoint = endpoint.strip('/')
        if endpoint.endswith(".px"):
            return endpoint[:-3]
        return f"{endpoint}.px"

    def get(self, endpoint):
        url = self._url(endpoint)
        response = requests.get(url, headers={"Accept": "application/json"})
        if response.status_code == 400:
            alt = self._alternate_endpoint(endpoint)
            if alt != endpoint:
                response = requests.get(self._url(alt), headers={"Accept": "application/json"})
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"GET {response.url} failed: {response.status_code} {response.text}",
                response=response,
            )
        response.raise_for_status()
        return response.json()

    def post(self, endpoint, json_body):
        url = self._url(endpoint)
        response = requests.post(url, json=json_body, headers={"Accept": "application/json"})
        if response.status_code == 400:
            alt = self._alternate_endpoint(endpoint)
            if alt != endpoint:
                response = requests.post(self._url(alt), json=json_body, headers={"Accept": "application/json"})
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"POST {response.url} failed: {response.status_code} {response.text}",
                response=response,
            )
        response.raise_for_status()
        return response.json()
