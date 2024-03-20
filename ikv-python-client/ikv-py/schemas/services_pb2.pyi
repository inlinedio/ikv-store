from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import empty_pb2 as _empty_pb2
import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HelloWorldRequest(_message.Message):
    __slots__ = ("echo",)
    ECHO_FIELD_NUMBER: _ClassVar[int]
    echo: str
    def __init__(self, echo: _Optional[str] = ...) -> None: ...

class HelloWorldResponse(_message.Message):
    __slots__ = ("echo",)
    ECHO_FIELD_NUMBER: _ClassVar[int]
    echo: str
    def __init__(self, echo: _Optional[str] = ...) -> None: ...

class HealthCheckResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: int
    def __init__(self, code: _Optional[int] = ...) -> None: ...

class UpsertFieldValuesRequest(_message.Message):
    __slots__ = ("userStoreContextInitializer", "timestamp", "document")
    USERSTORECONTEXTINITIALIZER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    userStoreContextInitializer: UserStoreContextInitializer
    timestamp: _timestamp_pb2.Timestamp
    document: _common_pb2.IKVDocumentOnWire
    def __init__(self, userStoreContextInitializer: _Optional[_Union[UserStoreContextInitializer, _Mapping]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., document: _Optional[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]] = ...) -> None: ...

class BatchUpsertFieldValuesRequest(_message.Message):
    __slots__ = ("userStoreContextInitializer", "timestamp", "documents")
    USERSTORECONTEXTINITIALIZER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    userStoreContextInitializer: UserStoreContextInitializer
    timestamp: _timestamp_pb2.Timestamp
    documents: _containers.RepeatedCompositeFieldContainer[_common_pb2.IKVDocumentOnWire]
    def __init__(self, userStoreContextInitializer: _Optional[_Union[UserStoreContextInitializer, _Mapping]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., documents: _Optional[_Iterable[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]]] = ...) -> None: ...

class DeleteFieldValueRequest(_message.Message):
    __slots__ = ("userStoreContextInitializer", "timestamp", "documentId", "fieldNames")
    USERSTORECONTEXTINITIALIZER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTID_FIELD_NUMBER: _ClassVar[int]
    FIELDNAMES_FIELD_NUMBER: _ClassVar[int]
    userStoreContextInitializer: UserStoreContextInitializer
    timestamp: _timestamp_pb2.Timestamp
    documentId: _common_pb2.IKVDocumentOnWire
    fieldNames: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, userStoreContextInitializer: _Optional[_Union[UserStoreContextInitializer, _Mapping]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., documentId: _Optional[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]] = ..., fieldNames: _Optional[_Iterable[str]] = ...) -> None: ...

class BatchDeleteFieldValuesRequest(_message.Message):
    __slots__ = ("userStoreContextInitializer", "timestamp", "documentIds", "fieldNames")
    USERSTORECONTEXTINITIALIZER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTIDS_FIELD_NUMBER: _ClassVar[int]
    FIELDNAMES_FIELD_NUMBER: _ClassVar[int]
    userStoreContextInitializer: UserStoreContextInitializer
    timestamp: _timestamp_pb2.Timestamp
    documentIds: _containers.RepeatedCompositeFieldContainer[_common_pb2.IKVDocumentOnWire]
    fieldNames: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, userStoreContextInitializer: _Optional[_Union[UserStoreContextInitializer, _Mapping]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., documentIds: _Optional[_Iterable[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]]] = ..., fieldNames: _Optional[_Iterable[str]] = ...) -> None: ...

class DeleteDocumentRequest(_message.Message):
    __slots__ = ("userStoreContextInitializer", "timestamp", "documentId")
    USERSTORECONTEXTINITIALIZER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTID_FIELD_NUMBER: _ClassVar[int]
    userStoreContextInitializer: UserStoreContextInitializer
    timestamp: _timestamp_pb2.Timestamp
    documentId: _common_pb2.IKVDocumentOnWire
    def __init__(self, userStoreContextInitializer: _Optional[_Union[UserStoreContextInitializer, _Mapping]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., documentId: _Optional[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]] = ...) -> None: ...

class BatchDeleteDocumentsRequest(_message.Message):
    __slots__ = ("userStoreContextInitializer", "timestamp", "documentIds")
    USERSTORECONTEXTINITIALIZER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTIDS_FIELD_NUMBER: _ClassVar[int]
    userStoreContextInitializer: UserStoreContextInitializer
    timestamp: _timestamp_pb2.Timestamp
    documentIds: _containers.RepeatedCompositeFieldContainer[_common_pb2.IKVDocumentOnWire]
    def __init__(self, userStoreContextInitializer: _Optional[_Union[UserStoreContextInitializer, _Mapping]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., documentIds: _Optional[_Iterable[_Union[_common_pb2.IKVDocumentOnWire, _Mapping]]] = ...) -> None: ...

class DropFieldsRequest(_message.Message):
    __slots__ = ("userStoreContextInitializer", "timestamp", "field_names", "field_name_prefixes", "drop_all")
    USERSTORECONTEXTINITIALIZER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    FIELD_NAMES_FIELD_NUMBER: _ClassVar[int]
    FIELD_NAME_PREFIXES_FIELD_NUMBER: _ClassVar[int]
    DROP_ALL_FIELD_NUMBER: _ClassVar[int]
    userStoreContextInitializer: UserStoreContextInitializer
    timestamp: _timestamp_pb2.Timestamp
    field_names: _containers.RepeatedScalarFieldContainer[str]
    field_name_prefixes: _containers.RepeatedScalarFieldContainer[str]
    drop_all: bool
    def __init__(self, userStoreContextInitializer: _Optional[_Union[UserStoreContextInitializer, _Mapping]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., field_names: _Optional[_Iterable[str]] = ..., field_name_prefixes: _Optional[_Iterable[str]] = ..., drop_all: bool = ...) -> None: ...

class FieldSchema(_message.Message):
    __slots__ = ("name", "id", "fieldType")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    FIELDTYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    id: int
    fieldType: _common_pb2.FieldType
    def __init__(self, name: _Optional[str] = ..., id: _Optional[int] = ..., fieldType: _Optional[_Union[_common_pb2.FieldType, str]] = ...) -> None: ...

class GetUserStoreConfigRequest(_message.Message):
    __slots__ = ("userStoreContextInitializer",)
    USERSTORECONTEXTINITIALIZER_FIELD_NUMBER: _ClassVar[int]
    userStoreContextInitializer: UserStoreContextInitializer
    def __init__(self, userStoreContextInitializer: _Optional[_Union[UserStoreContextInitializer, _Mapping]] = ...) -> None: ...

class GetUserStoreConfigResponse(_message.Message):
    __slots__ = ("globalConfig",)
    GLOBALCONFIG_FIELD_NUMBER: _ClassVar[int]
    globalConfig: _common_pb2.IKVStoreConfig
    def __init__(self, globalConfig: _Optional[_Union[_common_pb2.IKVStoreConfig, _Mapping]] = ...) -> None: ...

class Status(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UserStoreContextInitializer(_message.Message):
    __slots__ = ("credentials", "storeName")
    CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    STORENAME_FIELD_NUMBER: _ClassVar[int]
    credentials: AccountCredentials
    storeName: str
    def __init__(self, credentials: _Optional[_Union[AccountCredentials, _Mapping]] = ..., storeName: _Optional[str] = ...) -> None: ...

class AccountCredentials(_message.Message):
    __slots__ = ("accountId", "accountPasskey")
    ACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNTPASSKEY_FIELD_NUMBER: _ClassVar[int]
    accountId: str
    accountPasskey: str
    def __init__(self, accountId: _Optional[str] = ..., accountPasskey: _Optional[str] = ...) -> None: ...
