import json
import grpc
import schemas.services_pb2_grpc as services_pb2_grpc
import schemas.services_pb2 as services_pb2
import schemas.common_pb2 as common_pb2

from google.protobuf import timestamp_pb2
from grpc_status import rpc_status

from client import IKVWriter
from document import IKVDocument
from typing import List, Optional
from clientoptions import ClientOptions
from utils import is_valid_str_or_raise

# grpc call retry policy
retry_policy = json.dumps(
    {
        "methodConfig": [{
            "name": [{"service": "ikvschemas.InlineKVWriteService"}],
            "retryPolicy": {
			  "maxAttempts": 3,
			  "initialBackoff": "0.01s",
			  "maxBackoff": "0.01s",
			  "backoffMultiplier": 1.0,
			  "retryableStatusCodes": [ "UNAVAILABLE" ]
            },
        }]
    }
)

class IKVWriterImpl(IKVWriter):
    def __init__(self, client_options: ClientOptions):
        if client_options is None:
            raise TypeError("client_options are required and can't be None")
        self.client_options = client_options
        
        # initialize auth context initializer
        account_id = is_valid_str_or_raise(client_options.get_ikv_config().stringConfigs["account_id"])
        account_passkey = is_valid_str_or_raise(client_options.get_ikv_config().stringConfigs["account_passkey"])
        credentials: services_pb2.AccountCredentials = \
            services_pb2.AccountCredentials(accountId=account_id, accountPasskey=account_passkey)
        store_name = is_valid_str_or_raise(client_options.get_ikv_config().stringConfigs["store_name"])
        self.user_store_context_initializer: services_pb2.UserStoreContextInitializer = \
            services_pb2.UserStoreContextInitializer(credentials=credentials, storeName=store_name)

        self.channel = None
        self.stub = None

    def startup(self):
        if self.channel is None:
            options = []
            options.append(("grpc.service_config", retry_policy))
            self.channel = grpc.secure_channel(target="gateway.inlined.io:443", credentials=grpc.ssl_channel_credentials(), options=options)
            self.stub = services_pb2_grpc.InlineKVWriteServiceStub(self.channel)

    def shutdown(self):
        if self.channel is not None:
            self.channel.close()
            self.channel = None
            self.stub = None

    def fetch_server_supplied_config(self) -> Optional[common_pb2.IKVStoreConfig]:
        request: services_pb2.GetUserStoreConfigRequest = services_pb2.GetUserStoreConfigRequest(\
            userStoreContextInitializer=self.user_store_context_initializer)

        try:
            response: services_pb2.GetUserStoreConfigResponse = self.stub.getUserStoreConfig(request)
            return response.globalConfig
        except grpc.RpcError as rpc_error:
            status = rpc_status.from_call(rpc_error)
            raise RuntimeError(f"Unexpected failure, status code: {status.code} message: {status.message}")

    def upsert_fields(self, document: IKVDocument):
        if document is None or document.len() < 1:
            raise ValueError("Not a valid document, must contain primary-key")

        request = services_pb2.UpsertFieldValuesRequest(\
            userStoreContextInitializer=self.user_store_context_initializer, timestamp=self._create_timestamp(), \
                document=document.as_ikv_document_on_wire())
        
        try:
            self.stub.upsertFieldValues(request)
        except grpc.RpcError as rpc_error:
            status = rpc_status.from_call(rpc_error)
            raise RuntimeError(f"Unexpected failure, status code: {status.code} message: {status.message}")

    def delete_fields(self, document: IKVDocument, fields_to_delete: List[str]):
        if document is None or document.len() < 1:
            raise ValueError("Not a valid document, must contain primary-key")
        
        if fields_to_delete is None or len(fields_to_delete) == 0:
            return

        request = services_pb2.DeleteFieldValueRequest(\
            userStoreContextInitializer=self.user_store_context_initializer, timestamp=self._create_timestamp(), \
                documentId=document.as_ikv_document_on_wire(), fieldNames=fields_to_delete)

        try:
            self.stub.deleteFieldValues(request)
        except grpc.RpcError as rpc_error:
            status = rpc_status.from_call(rpc_error)
            raise RuntimeError(f"Unexpected failure, status code: {status.code} message: {status.message}")

    def delete_document(self, document: IKVDocument):
        if document is None or document.len() < 1:
            raise ValueError("Not a valid document, must contain primary-key")
        
        request = services_pb2.DeleteDocumentRequest(\
            userStoreContextInitializer=self.user_store_context_initializer, timestamp=self._create_timestamp(), \
                documentId=document.as_ikv_document_on_wire())

        try:
            self.stub.deleteDocument(request)
        except grpc.RpcError as rpc_error:
            status = rpc_status.from_call(rpc_error)
            raise RuntimeError(f"Unexpected failure, status code: {status.code} message: {status.message}")

    def drop_fields_by_name(self, field_names: List[str]):
        if field_names is None or len(field_names) == 0:
            return

        request: services_pb2.DropFieldsRequest = services_pb2.DropFieldsRequest(\
            userStoreContextInitializer=self.user_store_context_initializer, timestamp=self._create_timestamp(), \
                field_names=field_names, drop_all=False)
        
        try:
            self.stub.dropFields(request)
        except grpc.RpcError as rpc_error:
            status = rpc_status.from_call(rpc_error)
            raise RuntimeError(f"Unexpected failure, status code: {status.code} message: {status.message}")

    def drop_fields_by_name_prefix(self, field_name_prefixes: List[str]):
        if field_name_prefixes is None or len(field_name_prefixes) == 0:
            return

        request: services_pb2.DropFieldsRequest = services_pb2.DropFieldsRequest(\
            userStoreContextInitializer=self.user_store_context_initializer, timestamp=self._create_timestamp(), \
                field_name_prefixes=field_name_prefixes, drop_all=False)
        
        try:
            self.stub.dropFields(request)
        except grpc.RpcError as rpc_error:
            status = rpc_status.from_call(rpc_error)
            raise RuntimeError(f"Unexpected failure, status code: {status.code} message: {status.message}")

    def drop_all_fields(self):
        request: services_pb2.DropFieldsRequest = services_pb2.DropFieldsRequest(\
            userStoreContextInitializer=self.user_store_context_initializer, timestamp=self._create_timestamp(), drop_all=True)
        
        try:
            self.stub.dropFields(request)
        except grpc.RpcError as rpc_error:
            status = rpc_status.from_call(rpc_error)
            raise RuntimeError(f"Unexpected failure, status code: {status.code} message: {status.message}")
    
    def _create_timestamp(self):
        timestamp = timestamp_pb2.Timestamp()
        timestamp.GetCurrentTime()
        return timestamp