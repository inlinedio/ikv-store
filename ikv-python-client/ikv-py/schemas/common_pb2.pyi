from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FieldType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNKNOWN: _ClassVar[FieldType]
    INT32: _ClassVar[FieldType]
    INT64: _ClassVar[FieldType]
    FLOAT32: _ClassVar[FieldType]
    FLOAT64: _ClassVar[FieldType]
    STRING: _ClassVar[FieldType]
    BYTES: _ClassVar[FieldType]
    BOOLEAN: _ClassVar[FieldType]
UNKNOWN: FieldType
INT32: FieldType
INT64: FieldType
FLOAT32: FieldType
FLOAT64: FieldType
STRING: FieldType
BYTES: FieldType
BOOLEAN: FieldType

class IKVDocumentOnWire(_message.Message):
    __slots__ = ("document",)
    class DocumentEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: FieldValue
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[FieldValue, _Mapping]] = ...) -> None: ...
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    document: _containers.MessageMap[str, FieldValue]
    def __init__(self, document: _Optional[_Mapping[str, FieldValue]] = ...) -> None: ...

class FieldValue(_message.Message):
    __slots__ = ("fieldType", "value")
    FIELDTYPE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    fieldType: FieldType
    value: bytes
    def __init__(self, fieldType: _Optional[_Union[FieldType, str]] = ..., value: _Optional[bytes] = ...) -> None: ...

class IKVStoreConfig(_message.Message):
    __slots__ = ("stringConfigs", "intConfigs", "floatConfigs", "bytesConfigs", "booleanConfigs")
    class StringConfigsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class IntConfigsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    class FloatConfigsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    class BytesConfigsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class BooleanConfigsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bool
        def __init__(self, key: _Optional[str] = ..., value: bool = ...) -> None: ...
    STRINGCONFIGS_FIELD_NUMBER: _ClassVar[int]
    INTCONFIGS_FIELD_NUMBER: _ClassVar[int]
    FLOATCONFIGS_FIELD_NUMBER: _ClassVar[int]
    BYTESCONFIGS_FIELD_NUMBER: _ClassVar[int]
    BOOLEANCONFIGS_FIELD_NUMBER: _ClassVar[int]
    stringConfigs: _containers.ScalarMap[str, str]
    intConfigs: _containers.ScalarMap[str, int]
    floatConfigs: _containers.ScalarMap[str, float]
    bytesConfigs: _containers.ScalarMap[str, bytes]
    booleanConfigs: _containers.ScalarMap[str, bool]
    def __init__(self, stringConfigs: _Optional[_Mapping[str, str]] = ..., intConfigs: _Optional[_Mapping[str, int]] = ..., floatConfigs: _Optional[_Mapping[str, float]] = ..., bytesConfigs: _Optional[_Mapping[str, bytes]] = ..., booleanConfigs: _Optional[_Mapping[str, bool]] = ...) -> None: ...
