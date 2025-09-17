# Hagstofan/base_data_source.py
from abc import ABC, abstractmethod

class BaseDataSource(ABC):
    def __init__(self, client, endpoint):
        self.client = client
        self.endpoint = endpoint

    def get_data(self, json_body):
        return self.client.post(self.endpoint, json_body)
