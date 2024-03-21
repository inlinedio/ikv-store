from typing import Tuple
from client import IKVReader
from clientoptions import ClientOptions

import native_reader

class IKVReaderImpl(IKVReader):
    def __init__(self, client_options: ClientOptions) -> None:
        self.client_options = client_options
        self.native_reader = native_reader.NativeReader("/path/to/dll")

    def startup(self, client_options: ClientOptions):
        # TODO: download dll dynamically
        ikv_config_bytes = bytes(client_options.get_ikv_config().SerializeToString())
        self.native_reader.open(ikv_config_bytes)

    def shutdown(self):
        self.native_reader.close()

    # nullable return type
    def get_bytes_value(self, primary_key, field_name: str) -> bytes:
        if isinstance(primary_key, str):
            return self.native_reader.get_field_value(bytes(primary_key.encode('utf-8')), field_name)
        
        if isinstance(primary_key, bytes):
            return self.native_reader.get_field_value(primary_key, field_name)
        
        if isinstance(primary_key, bytearray):
            return self.native_reader.get_field_value(bytes(primary_key), field_name)
        
        raise TypeError("unsupported primary_key type: {}, supported: str/bytes/bytearray".format(type(primary_key)))

    def get_string_value(self, primary_key, field_name: str) -> str:
        maybe_value = self.get_bytes_value(primary_key, field_name)
        
        if maybe_value is None:
            return None
        
        return maybe_value.decode('utf-8')