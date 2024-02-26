//go:build !cgo

package ikvclient

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct BytesBuffer {
  int32_t length;
  uint8_t *start;
} BytesBuffer;

int64_t health_check(const char *input);
int64_t open_index(const char *config, int32_t config_len);
void close_index(int64_t handle);
BytesBuffer get_field_value(int64_t handle, const char *pkey, int32_t pkey_len, const char *field_name);
void free_bytes_buffer(BytesBuffer buf);

// function pointer type
typedef int64_t (*go_health_check_type)(const char*);
// wrapper function
int64_t go_health_check(void* f, const char* s) {
    return ((go_health_check_type) f)(s);
}

// function pointer type
typedef int64_t (*go_open_index_type)(const char*, int32_t);
// wrapper function
int64_t go_open_index(void* f, const char* config, int32_t config_len) {
    return ((go_open_index_type) f)(config, config_len);
}

// function pointer type
typedef void (*go_close_index_type)(int64_t);
// wrapper function
void go_close_index(void* f, int64_t handle) {
    ((go_close_index_type) f)(handle);
}

// function pointer type
typedef BytesBuffer (*go_get_field_value_type)(int64_t, const char*, int32_t, const char*);
// wrapper function
BytesBuffer go_get_field_value(void* f, int64_t handle, const char *pkey, int32_t pkey_len, const char *field_name) {
    return ((go_get_field_value_type) f)(handle, pkey, pkey_len, field_name);
}

// function pointer type
typedef void (*go_free_bytes_buffer_type)(BytesBuffer);
// wrapper function
void go_free_bytes_buffer(void* f, BytesBuffer buffer) {
    ((go_free_bytes_buffer_type) f)(buffer);
}

*/
import "C"
import (
	"errors"
	"unsafe"
)

type NativeReaderV2 struct {
	// index handle
	handle int64

	dll_path      string
	dll_path_cstr unsafe.Pointer

	// native function pointers
	health_check_fptr      unsafe.Pointer
	open_index_fptr        unsafe.Pointer
	close_index_fptr       unsafe.Pointer
	get_field_value_fptr   unsafe.Pointer
	free_bytes_buffer_fptr unsafe.Pointer
}

func NewNativeReaderV2(dllPath string) *NativeReaderV2 {
	nr := &NativeReaderV2{handle: -1, dll_path: dllPath}
	return nr
}

func (nr *NativeReaderV2) HealthCheck(input string) (bool, error) {
	return false, errors.New("Enable CGO for IKV native library io")
}

func (nr *NativeReaderV2) Open(config []byte) error {
	return errors.New("Enable CGO for IKV native library io")
}

func (nr *NativeReaderV2) Close() error {
	return errors.New("Enable CGO for IKV native library io")
}

func (nr *NativeReaderV2) GetFieldValue(primaryKey []byte, fieldName string) []byte {
	return make([]byte, 0)
}
