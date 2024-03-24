from google.protobuf import timestamp_pb2 as _timestamp_pb2
import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EventHeader(_message.Message):
    __slots__ = ("sourceTimestamp",)
    SOURCETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    sourceTimestamp: _timestamp_pb2.Timestamp
    def __init__(self, sourceTimestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class IKVDataEvent(_message.Message):
    __slots__ = ("eventHeader", "upsertDocumentFieldsEvent", "deleteDocumentFieldsEvent", "deleteDocumentEvent", "dropFieldEvent")
    EVENTHEADER_FIELD_NUMBER: _ClassVar[int]
    UPSERTDOCUMENTFIELDSEVENT_FIELD_NUMBER: _ClassVar[int]
    DELETEDOCUMENTFIELDSEVENT_FIELD_NUMBER: _ClassVar[int]
    DELETEDOCUMENTEVENT_FIELD_NUMBER: _ClassVar[int]
    DROPFIELDEVENT_FIELD_NUMBER: _ClassVar[int]
    eventHeader: EventHeader
    upsertDocumentFieldsEvent: UpsertDocumentFieldsEvent
    deleteDocumentFieldsEvent: DeleteDocumentFieldsEvent
    deleteDocumentEvent: DeleteDocumentEvent
    dropFieldEvent: DropFieldEvent
    def __init__(self, eventHeader: _Optional[_Union[EventHeader, _Mapping]] = ..., upsertDocumentFieldsEvent: _Optional[_Union[UpsertDocumentFieldsEvent, _Mapping]] = ..., deleteDocumentFieldsEvent: _Optional[_Union[DeleteDocumentFieldsEvent, _Mapping]] = ..., deleteDocumentEvent: _Optional[_Union[DeleteDocumentEvent, _Mapping]] = ..., dropFieldEvent: _Optional[_Union[DropFieldEvent, _Mapping]] = ...) -> None: ...

class UpsertDocumentFieldsEvent(_message.Message):
    __slots__ = ("document",)
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    document: _common_pb2.IKVDocumentOnWire
    def __init__(self, document: _Optional[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]] = ...) -> None: ...

class DeleteDocumentFieldsEvent(_message.Message):
    __slots__ = ("documentId", "fieldsToDelete")
    DOCUMENTID_FIELD_NUMBER: _ClassVar[int]
    FIELDSTODELETE_FIELD_NUMBER: _ClassVar[int]
    documentId: _common_pb2.IKVDocumentOnWire
    fieldsToDelete: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, documentId: _Optional[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]] = ..., fieldsToDelete: _Optional[_Iterable[str]] = ...) -> None: ...

class DeleteDocumentEvent(_message.Message):
    __slots__ = ("documentId",)
    DOCUMENTID_FIELD_NUMBER: _ClassVar[int]
    documentId: _common_pb2.IKVDocumentOnWire
    def __init__(self, documentId: _Optional[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]] = ...) -> None: ...

class DropFieldEvent(_message.Message):
    __slots__ = ("field_names", "field_name_prefixes", "drop_all")
    FIELD_NAMES_FIELD_NUMBER: _ClassVar[int]
    FIELD_NAME_PREFIXES_FIELD_NUMBER: _ClassVar[int]
    DROP_ALL_FIELD_NUMBER: _ClassVar[int]
    field_names: _containers.RepeatedScalarFieldContainer[str]
    field_name_prefixes: _containers.RepeatedScalarFieldContainer[str]
    drop_all: bool
    def __init__(self, field_names: _Optional[_Iterable[str]] = ..., field_name_prefixes: _Optional[_Iterable[str]] = ..., drop_all: bool = ...) -> None: ...
