from typing import Iterator, List, Optional
from cffi import FFI
import utils
from utils import is_valid_str_or_raise, is_valid_bytes_or_raise

EMPTY_ITERATOR = iter([])

ffi = FFI()
ffi.cdef("""
    // Start of common C code (Go, Python)
    typedef struct BytesBuffer {
    int32_t length;
    uint8_t *start;
    } BytesBuffer;

    typedef struct IndexHandle {
    int64_t handle;
    int64_t status;
    } IndexHandle;

    int64_t health_check(const char *input);
    
    IndexHandle open_index_v2(const char *config, int32_t config_len);
    
    void close_index(int64_t handle);
    
    BytesBuffer get_field_value(int64_t handle, const char *pkey, int32_t pkey_len, const char *field_name);

    BytesBuffer multiget_field_values(int64_t handle, const char *pkeys, int32_t pkeys_len, const char *field_names, int32_t field_names_len);

    void free_bytes_buffer(BytesBuffer buf);
    // End of common C code (Go, Python)
""")

class NativeReader:
    def __init__(self, dll_path: str):
        self.index_handle = -1
        self.dll = ffi.dlopen(dll_path)
    
    def health_check(self, input: str) -> bool:
        # this will be auto free'd with python gc
        c_input = ffi.new("char[]", is_valid_str_or_raise(input).encode('utf-8'))
        status = self.dll.health_check(c_input)
        return status == 0

    def open(self, ikv_config: bytes):
        if self.index_handle != -1:
            return

        # this will be auto free'd with python gc
        c_ikv_config = ffi.new("char[]", is_valid_bytes_or_raise(ikv_config))
        index_handle = self.dll.open_index_v2(c_ikv_config, len(ikv_config))

        if index_handle.status == 0:
            self.index_handle = index_handle.handle
            return
        
        # error
        raise RuntimeError("Cannot open IKV reader, error code: {}".format(index_handle.status))

    def close(self):
        if self.index_handle == -1:
            return

        self.dll.close_index(self.index_handle)

    def get_bytes_field_value(self, primary_key: bytes, field_name: str) -> Optional[bytes]:
        c_primary_key = ffi.from_buffer(primary_key)
        c_field_name = ffi.from_buffer(field_name.encode('utf-8'))

        bytes_buffer = self.dll.get_field_value(self.index_handle, c_primary_key, len(primary_key), c_field_name)
        
        value_len = bytes_buffer.length
        value_start = bytes_buffer.start

        if value_len == 0 or value_start == ffi.NULL:
            return None
        
        # copy from the pointer
        value = bytes(ffi.buffer(value_start, value_len))

        # release rust allocated objects
        self.dll.free_bytes_buffer(bytes_buffer)

        return value

    def get_string_field_value(self, primary_key: bytes, field_name: str) -> Optional[str]:
        c_primary_key = ffi.from_buffer(primary_key)
        c_field_name = ffi.from_buffer(field_name.encode('utf-8'))

        bytes_buffer = self.dll.get_field_value(self.index_handle, c_primary_key, len(primary_key), c_field_name)

        value_len = bytes_buffer.length
        value_start = bytes_buffer.start

        if value_len == 0 or value_start == ffi.NULL:
            return None

        # zero copy decoding into str
        value = str(memoryview(ffi.buffer(value_start, value_len)), 'utf-8')

        # release rust allocated objects
        self.dll.free_bytes_buffer(bytes_buffer)

        return value

    def multiget_bytes_field_values(self, bytes_primary_keys: List[bytes] = [], str_primary_keys: List[str] = [],
            field_names: List[str] = []) -> Iterator[Optional[bytes]]:
        """ native muti-get wrapper. """

        num_values = (len(bytes_primary_keys) + len(str_primary_keys)) * len(field_names)
        if num_values == 0:
            return EMPTY_ITERATOR

        concat_primary_keys: bytearray = utils.concat_as_utf8_with_size_prefix(bytes_primary_keys, str_primary_keys)
        c_concat_primary_keys = ffi.from_buffer(concat_primary_keys)
        concat_field_names: bytearray = utils.concat_strings_with_size_prefix(field_names)
        c_concat_field_names = ffi.from_buffer(concat_field_names)

        bytes_buffer = self.dll.multiget_field_values(self.index_handle, c_concat_primary_keys,
            len(concat_primary_keys), c_concat_field_names, len(concat_field_names))

        value_len = bytes_buffer.length
        value_start = bytes_buffer.start
        if value_len == 0 or value_start == ffi.NULL:
            return iter([None] * num_values)

        # copy from the pointer
        value = bytes(ffi.buffer(value_start, value_len))

        # release rust allocated objects
        self.dll.free_bytes_buffer(bytes_buffer)

        # return iterator over `value`
        return utils.unpack_size_prefixed_bytes_as_bytes(value)

    def multiget_str_field_values(self, bytes_primary_keys: List[bytes] = [], str_primary_keys: List[str] = [],
            field_names: List[str] = []) -> Iterator[Optional[str]]:
        """ native muti-get wrapper. """

        num_values = (len(bytes_primary_keys) + len(str_primary_keys)) * len(field_names)
        if num_values == 0:
            return EMPTY_ITERATOR

        concat_primary_keys: bytearray = utils.concat_as_utf8_with_size_prefix(bytes_primary_keys, str_primary_keys)
        c_concat_primary_keys = ffi.from_buffer(concat_primary_keys)
        concat_field_names: bytearray = utils.concat_strings_with_size_prefix(field_names)
        c_concat_field_names = ffi.from_buffer(concat_field_names)

        bytes_buffer = self.dll.multiget_field_values(self.index_handle, c_concat_primary_keys,
            len(concat_primary_keys), c_concat_field_names, len(concat_field_names))

        value_len = bytes_buffer.length
        value_start = bytes_buffer.start
        if value_len == 0 or value_start == ffi.NULL:
            return iter([None] * num_values)

        # copy from the pointer
        value = bytes(ffi.buffer(value_start, value_len))

        # release rust allocated objects
        self.dll.free_bytes_buffer(bytes_buffer)

        # return iterator over `value`
        return utils.unpack_size_prefixed_bytes_as_str(value)