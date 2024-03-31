from abc import ABC, abstractmethod
from clientoptions import ClientOptions
from document import IKVDocument
from typing import Iterator, List, Optional

class IKVReader(ABC):
    """
    See https://docs.inlined.io for detailed Python usage guide.
    See factory.py for instantiating a concrete reader instance.

    Reader client over a provisioned IKV store.
    You should reuse an instantiated client as it holds an embedded database instance.
    Lifecycle (startup/shutdown) of a reader client should be tied with application lifecycle.
    """
    @abstractmethod
    def startup(self):
        """
        Blocking startup, initializes the embedded store by pulling latest data locally.
        Do not invoke other functions before startup is finished.
        """
        raise NotImplementedError("cannot be used directly, use IKVReaderImpl")

    @abstractmethod
    def shutdown(self):
        """
        Shutdown embedded store - ex. during application exit.
        """
        raise NotImplementedError("cannot be used directly, use IKVReaderImpl")

    @abstractmethod
    def get_bytes_value(self, primary_key, field_name: str) -> Optional[bytes]:
        """
        Fetch an inner field of type bytes, by providing the primary-key for the document and the field-name.
        Please note IKV readers are eventually-consistent in terms of "reading your writes".
        There can be a very small delay between writing document (with IKVWriter) and reading them.

        args:
            primary_key: document identifier (bytes or str)
            field_name: field identifier
        returns:
            field value if it exists (bytes or None)
        """
        raise NotImplementedError("cannot be used directly, use IKVReaderImpl")

    @abstractmethod
    def multiget_bytes_values(self, bytes_primary_keys: List[bytes] = [], str_primary_keys: List[str] = [],\
            field_names: List[str] = []) -> Iterator[Optional[bytes]]:
        raise NotImplementedError("cannot be used directly, use IKVReaderImpl")

    @abstractmethod
    def multiget_string_values(self, bytes_primary_keys: List[bytes] = [], str_primary_keys: List[str] = [],\
            field_names: List[str] = []) -> Iterator[Optional[str]]:
        raise NotImplementedError("cannot be used directly, use IKVReaderImpl")

    @abstractmethod
    def get_string_value(self, primary_key, field_name: str) -> Optional[str]:
        """
        Fetch an inner field of type str, by providing the primary-key for the document and the field-name.
        Please note IKV readers are eventually-consistent in terms of "reading your writes".
        There can be a very small delay between writing document (with IKVWriter) and reading them.

        args:
            primary_key: document identifier (bytes or str)
            field_name: field identifier
        returns:
            field value if it exists (str or None)
        """
        raise NotImplementedError("cannot be used directly, use IKVReaderImpl")


class IKVWriter(ABC):
    """
    See https://docs.inlined.io for detailed Python usage guide.
    See factory.py for instantiating a concrete writer instance.

    Writer client over a provisioned IKV store.
    You should reuse an instantiated client.
    Lifecycle (startup/shutdown) of a writer client should be tied with application lifecycle.
    """
    @abstractmethod
    def startup(self):
        """
        Initializes the writer client. Do not invoke other functions before startup is finished.
        """
        raise NotImplementedError("cannot be used directly, use IKVWriterImpl")

    @abstractmethod
    def shutdown(self):
        """
        Shutdown writer client - ex. during application exit.
        """
        raise NotImplementedError("cannot be used directly, use IKVWriterImpl")
    
    @abstractmethod
    def upsert_fields(self, document: IKVDocument):
        """
        Upsert (insert or update) fields for a document.
        Provided `document` must contain the value of the primary-key and (if-applicable) partitioning-key.
        See document.py for creating IKVDocument objects.

        Different values for the same primary-key are aggregated by unionizing distincts and overwriting duplicates. Example -
        upsert: {"pkey": 0, "key1": "value1"}, followed by upsert: {"pkey": 0, "key2": "value2"}
        results in {"pkey": 0, "key1": "value1", "key2": "value2"} being saved.
        """
        raise NotImplementedError("cannot be used directly, use IKVWriterImpl")

    @abstractmethod
    def delete_fields(self, document: IKVDocument, fields_to_delete: List[str]):
        """
        Delete specified fields from a document. NO OP if document or any field in `fields_to_delete` does not exist.
        Provided `document` must contain the value of the primary-key and (if-applicable) partitioning-key.
        See document.py for creating IKVDocument objects.
        """
        raise NotImplementedError("cannot be used directly, use IKVWriterImpl")

    @abstractmethod
    def delete_document(self, document: IKVDocument):
        """
        Delete a document (i.e. all of its fields) if it exists. NO OP if `document` is not indexed.
        Provided `document` must contain the value of the primary-key and (if-applicable) partitioning-key.
        See document.py for creating IKVDocument objects.
        """
        raise NotImplementedError("cannot be used directly, use IKVWriterImpl")

    @abstractmethod
    def drop_fields_by_name(self, field_names: List[str]):
        """
        Drop specified fields for all documents.
        Attempts to drop primary-key field are silently ignored (no error).
        """
        raise NotImplementedError("cannot be used directly, use IKVWriterImpl")

    @abstractmethod
    def drop_fields_by_name_prefix(self, field_name_prefixes: List[str]):
        """
        Drop specified fields for all documents, by specifying field name prefixes.
        Attempts to drop primary-key field are silently ignored (no error).
        """
        raise NotImplementedError("cannot be used directly, use IKVWriterImpl")
    
    @abstractmethod
    def drop_all_fields(self):
        """
        Drop all fields (except primary-key) for all documents.
        """
        raise NotImplementedError("cannot be used directly, use IKVWriterImpl")