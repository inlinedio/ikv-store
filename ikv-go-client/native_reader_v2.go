package ikvclient

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>

// Start of common C code (Go, Python)
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
BytesBuffer multiget_field_values(int64_t handle, const char *pkeys, int32_t pkeys_len, const char *field_names, int32_t field_names_len);
void free_bytes_buffer(BytesBuffer buf);
// End of common C code (Go, Python)

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
typedef BytesBuffer (*go_multiget_field_values_type)(int64_t, const char*, int32_t, const char*, int32_t);
// wrapper function
BytesBuffer go_multiget_field_values(void* f, int64_t handle, const char *pkeys, int32_t pkeys_len, const char *field_names, int32_t field_names_len) {
    return ((go_multiget_field_values_type) f)(handle, pkeys, pkeys_len, field_names, field_names_len);
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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

type NativeReaderV2 struct {
	// index handle
	handle int64

	dll_path      string
	dll_path_cstr unsafe.Pointer

	// native function pointers
	health_check_fptr          unsafe.Pointer
	open_index_fptr            unsafe.Pointer
	close_index_fptr           unsafe.Pointer
	get_field_value_fptr       unsafe.Pointer
	multiget_field_values_fptr unsafe.Pointer
	free_bytes_buffer_fptr     unsafe.Pointer
}

func NewNativeReaderV2(dllPath string) (*NativeReaderV2, error) {
	nr := &NativeReaderV2{handle: -1, dll_path: dllPath}

	// dynamic loading
	dllPath_cstr := C.CString(dllPath)

	dllhandle := C.dlopen(dllPath_cstr, C.RTLD_LAZY)
	if dllhandle == nil {
		return nil, errors.New("cannot load library")
	}

	// TODO: dll_path_cstr might not be required
	// also invoke free() on cstrings for function names below

	nr.dll_path_cstr = unsafe.Pointer(dllPath_cstr)

	// lookup function pointers
	nr.health_check_fptr = C.dlsym(dllhandle, C.CString("health_check"))
	nr.open_index_fptr = C.dlsym(dllhandle, C.CString("open_index"))
	nr.close_index_fptr = C.dlsym(dllhandle, C.CString("close_index"))
	nr.get_field_value_fptr = C.dlsym(dllhandle, C.CString("get_field_value"))
	nr.multiget_field_values_fptr = C.dlsym(dllhandle, C.CString("multiget_field_values"))
	nr.free_bytes_buffer_fptr = C.dlsym(dllhandle, C.CString("free_bytes_buffer"))

	return nr, nil
}

func (nr *NativeReaderV2) HealthCheck(input string) (bool, error) {
	cstr := C.CString(input)
	defer C.free(unsafe.Pointer(cstr))

	status := int64(C.go_health_check(nr.health_check_fptr, cstr))
	if status != 0 {
		return false, fmt.Errorf("failure status code. Expected: 0 Actual : %d", status)
	}

	return true, nil
}

func (nr *NativeReaderV2) Open(config []byte) error {
	if nr.handle != -1 {
		return nil
	}

	cbytes := C.CBytes(config)
	defer C.free(unsafe.Pointer(cbytes))

	handle := C.go_open_index(nr.open_index_fptr, (*C.char)(unsafe.Pointer(cbytes)), C.int32_t(len(config)))
	nr.handle = int64(handle)
	return nil
}

func (nr *NativeReaderV2) Close() error {
	if nr.handle == -1 {
		return nil
	}

	C.go_close_index(nr.close_index_fptr, C.int64_t(nr.handle))
	return nil
}

// returns nil []byte if value does not exist ("empty")
func (nr *NativeReaderV2) GetFieldValue(primaryKey []byte, fieldName string) []byte {
	primaryKey_cbytes := C.CBytes(primaryKey)
	defer C.free(unsafe.Pointer(primaryKey_cbytes))

	fieldName_cstr := C.CString(fieldName)
	defer C.free(unsafe.Pointer(fieldName_cstr))

	var bb C.BytesBuffer = C.go_get_field_value(
		nr.get_field_value_fptr,
		C.int64_t(nr.handle),
		(*C.char)(unsafe.Pointer(primaryKey_cbytes)),
		C.int32_t(len(primaryKey)),
		fieldName_cstr,
	)

	length := int32(bb.length)
	if length == 0 || bb.start == nil {
		return nil
	}

	// only need to free for non empty response bb
	defer C.go_free_bytes_buffer(nr.free_bytes_buffer_fptr, bb)

	src := unsafe.Slice((*byte)(bb.start), length)
	result := make([]byte, length)
	copy(result, src)
	return result
}

func (nr *NativeReaderV2) MultiGetFieldValues(numPrimaryKeys int32, sizePrefixedPrimaryKeys []byte, fieldNames []string) ([][]byte, error) {
	if numPrimaryKeys == 0 || len(fieldNames) == 0 {
		return make([][]byte, 0), nil
	}

	// concatenate field names by size prefixing them
	var capacity = 0
	for _, fieldName := range fieldNames {
		capacity += 4 + len(fieldName)
	}
	buff := bytes.NewBuffer(make([]byte, 0, capacity))

	for _, fieldName := range fieldNames {
		value := []byte(fieldName)
		binary.Write(buff, binary.LittleEndian, int32(len(value)))
		buff.Write([]byte(fieldName))
	}
	var sizePrefixedFieldNames = buff.Bytes()

	// c bytes
	primaryKeys_cbytes := C.CBytes(sizePrefixedPrimaryKeys)
	defer C.free(unsafe.Pointer(primaryKeys_cbytes))
	fieldNames_cbytes := C.CBytes(sizePrefixedFieldNames)
	defer C.free(unsafe.Pointer(fieldNames_cbytes))

	// make FFI call
	var bb C.BytesBuffer = C.go_multiget_field_values(
		nr.multiget_field_values_fptr,
		C.int64_t(nr.handle),
		(*C.char)(unsafe.Pointer(primaryKeys_cbytes)),
		C.int32_t(len(sizePrefixedPrimaryKeys)),
		(*C.char)(unsafe.Pointer(fieldNames_cbytes)),
		C.int32_t(len(sizePrefixedFieldNames)),
	)

	expectedNumResults := numPrimaryKeys * int32(len(fieldNames))

	length := int32(bb.length)
	if length == 0 || bb.start == nil {
		return make([][]byte, expectedNumResults), errors.New("unreachable - should only occur if primarykeys or fieldnames are empty")
	}

	// only need to free for non empty response bb
	defer C.go_free_bytes_buffer(nr.free_bytes_buffer_fptr, bb)

	src := unsafe.Slice((*byte)(bb.start), length)

	// unpack size prefixed results in `result` to [][]byte
	results := make([][]byte, expectedNumResults)
	var resultId int32 = 0

	bufReader := bytes.NewReader(src)

	for resultId < expectedNumResults {
		var size int32
		if err := binary.Read(bufReader, binary.LittleEndian, &size); err != nil {
			return make([][]byte, expectedNumResults), err
		}

		if size == -1 {
			results[resultId] = []byte(nil)
		} else if size == 0 {
			results[resultId] = make([]byte, 0)
		} else {
			results[resultId] = make([]byte, size)
			if _, err := bufReader.Read(results[resultId]); err != nil {
				return make([][]byte, expectedNumResults), err
			}
		}

		resultId++
	}

	return results, nil
}
