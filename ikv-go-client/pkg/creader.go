package ikvclient

/*
#cgo LDFLAGS: -L../bin -likv
#include "../bin/ikv.h"
*/
import "C"
import "unsafe"

func PrintHelloWorld() {
	cstr := C.CString("foobar")

	// #include <stdlib.h> for free() is in ikv.h
	defer C.free(unsafe.Pointer(cstr))

	C.hello_world(cstr)
}

type NativeReader struct {
}

func (nr *NativeReader) open(config []byte) (int64, error) {
	cbytes := C.CBytes(config)
	defer C.free(unsafe.Pointer(cbytes))
	handle := C.open_index((*C.char)(unsafe.Pointer(cbytes)), C.int32_t(len(config)))
	return int64(handle), nil
}

func (nr *NativeReader) close(handle int64) error {
	C.close_index(C.int64_t(handle))
	return nil
}

func (nr *NativeReader) getFieldValue(handle int64, primaryKey []byte, fieldName string) []byte {
	primaryKey_cbytes := C.CBytes(primaryKey)
	defer C.free(unsafe.Pointer(primaryKey_cbytes))

	fieldName_cstr := C.CString(fieldName)
	defer C.free(unsafe.Pointer(fieldName_cstr))

	var bb C.BytesBuffer
	bb = C.get_field_value(
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
