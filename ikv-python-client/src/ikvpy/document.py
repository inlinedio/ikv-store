from typing import Dict
from schemas.common_pb2 import FieldValue, IKVDocumentOnWire
from schemas.common_pb2 import STRING as FieldValueString
from schemas.common_pb2 import BYTES as FieldValueBytes
import utils as ikvutils

class IKVDocument:
    """ 
    Representation of documents for writing into IKV. 
    See IKVDocumentBuilder to instantiate. 
    """
    def __init__(self, fields: Dict[str, FieldValue]):
        """ DO NOT use directly, see IKVDocumentBuilder to instantiate."""
        if fields is None:
            raise ValueError("fields cannot be None")
        self.fields: Dict[str, FieldValue] = fields
    
    def len(self):
        return len(self.fields)
    
    def as_ikv_document_on_wire(self) -> IKVDocumentOnWire:
        return IKVDocumentOnWire(document=self.fields)

class IKVDocumentBuilder:
    def __init__(self):
        self.fields: Dict[str, FieldValue] = {}
    
    def put_string_field(self, name: str, value: str) -> 'IKVDocumentBuilder':
        name: str = ikvutils.is_valid_str_or_raise(name)
        value: bytes = ikvutils.is_valid_str_or_raise(value).encode('utf-8')

        field_value: FieldValue = FieldValue(fieldType=FieldValueString, value=value)
        self.fields[name] = field_value

        return self
    
    def put_bytes_field(self, name: str, value: bytes) -> 'IKVDocumentBuilder':
        name: str = ikvutils.is_valid_str_or_raise(name)
        value: bytes = ikvutils.is_valid_bytes_or_raise(value)

        field_value: FieldValue = FieldValue(fieldType=FieldValueBytes, value=value)
        self.fields[name] = field_value
        
        return self

    def build(self) -> IKVDocument:
        return IKVDocument(fields=self.fields)