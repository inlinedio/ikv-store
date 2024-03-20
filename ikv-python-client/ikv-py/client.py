from abc import ABC, abstractmethod
from clientoptions import ClientOptions
from document import IKVDocument
from typing import List, Tuple
from schemas.services_pb2 import HelloWorldRequest

class IKVReader(ABC):
    """
    IKV Reader client. 
    TODO(pugupta) - docstrings
    """
    @abstractmethod
    def startup(self):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def get_bytes_value(self, primary_key, field_name: str) -> Tuple[bool, bytearray]:
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def get_string_value(self, primary_key, field_name: str) -> Tuple[bool, str]:
        raise NotImplementedError("subclass must override")

class IKVWriter(ABC):
    """
    IKV Writer client. 
    TODO(pugupta) - docstrings
    """
    @abstractmethod
    def startup(self):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError("subclass must override")
    
    @abstractmethod
    def upsert_fields(self, document: IKVDocument):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def delete_fields(self, document: IKVDocument, fields_to_delete: List[str]):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def delete_document(self, document: IKVDocument):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def drop_fields_by_name(self, field_names: List[str]):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def drop_fields_by_name_prefix(self, field_name_prefixes: List[str]):
        raise NotImplementedError("subclass must override")
    
    @abstractmethod
    def drop_all_fields(self):
        raise NotImplementedError("subclass must override")

"""
Factory methods
"""
def create_new_reader(client_options: ClientOptions) -> IKVReader:
    pass

def create_new_writer(client_options: ClientOptions) -> IKVWriter:
    pass