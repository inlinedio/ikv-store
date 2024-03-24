from typing import Optional
from cffi import FFI
from utils import is_valid_str_or_raise
from utils import is_valid_bytes_or_raise


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
        # avoid runtime arg checks in hot path
        c_primary_key = ffi.new("char[]", primary_key)

        # TODO: see if we can create a pool of python->c strings
        c_field_name = ffi.new("char[]", field_name.encode('utf-8'))

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
        # avoid runtime arg checks in hot path
        c_primary_key = ffi.new("char[]", primary_key)

        # TODO: see if we can create a pool of python->c strings
        c_field_name = ffi.new("char[]", field_name.encode('utf-8'))

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