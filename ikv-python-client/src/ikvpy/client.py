from abc import ABC, abstractmethod
from clientoptions import ClientOptions
from document import IKVDocument
from typing import List, Optional

class IKVReader(ABC):
    @abstractmethod
    def startup(self):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def get_bytes_value(self, primary_key, field_name: str) -> Optional[bytes]:
        raise NotImplementedError("subclass must override")

    @abstractmethod
    def get_string_value(self, primary_key, field_name: str) -> Optional[str]:
        raise NotImplementedError("subclass must override")

class IKVWriter(ABC):
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