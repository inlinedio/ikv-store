//go:build linux && arm64

package objects

/*
#cgo LDFLAGS: -L./linux_arm64 -likv
#include "./ikv.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func HealthCheck(input string) (bool, error) {
	cstr := C.CString(input)

	// #include <stdlib.h> for free() is in ikv.h
	defer C.free(unsafe.Pointer(cstr))

	status := int64(C.health_check(cstr))
	if status != 0 {
		return false, fmt.Errorf("Failure status code. Expected: 0 Actual : %d", status)
	}

	return true, nil
}

func Open(config []byte) (int64, error) {
	cbytes := C.CBytes(config)
	defer C.free(unsafe.Pointer(cbytes))
	handle := C.open_index((*C.char)(unsafe.Pointer(cbytes)), C.int32_t(len(config)))
	return int64(handle), nil
}

func Close(handle int64) error {
	C.close_index(C.int64_t(handle))
	return nil
}

func GetFieldValue(handle int64, primaryKey []byte, fieldName string) []byte {
	primaryKey_cbytes := C.CBytes(primaryKey)
	defer C.free(unsafe.Pointer(primaryKey_cbytes))

	fieldName_cstr := C.CString(fieldName)
	defer C.free(unsafe.Pointer(fieldName_cstr))

	var bb C.BytesBuffer = C.get_field_value(
		C.int64_t(handle),
		(*C.char)(unsafe.Pointer(primaryKey_cbytes)),
		C.int32_t(len(primaryKey)),
		fieldName_cstr,
	)
	defer C.free_bytes_buffer(bb)

	length := int32(bb.length)
	if length == 0 || bb.start == nil {
		return nil
	}

	src := unsafe.Slice((*byte)(bb.start), length)
	result := make([]byte, length)
	copy(result, src)
	return result
}
