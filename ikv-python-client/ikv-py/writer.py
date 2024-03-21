from client import IKVWriter
from document import IKVDocument
from typing import List

class IKVWriterImpl(IKVWriter):
    def startup(self):
        pass

    def shutdown(self):
        pass

    def upsert_fields(self, document: IKVDocument):
        pass

    def delete_fields(self, document: IKVDocument, fields_to_delete: List[str]):
        pass

    def delete_document(self, document: IKVDocument):
        pass

    def drop_fields_by_name(self, field_names: List[str]):
        pass

    def drop_fields_by_name_prefix(self, field_name_prefixes: List[str]):
        pass

    def drop_all_fields(self):
        pass