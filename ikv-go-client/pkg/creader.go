package ikvclient

/*
#cgo LDFLAGS: -L/Users/pushkar/projects/ikv-store/ikv/target/release -likv
#include <stdlib.h>
#include "/Users/pushkar/projects/ikv-store/ikv/src/ffi/c_api.h"
*/
import "C"
import "unsafe"

func PrintHelloWorld() {
	cstr := C.CString("foobar")
	defer C.free(unsafe.Pointer(cstr))
	C.hello_world_a(cstr)
	//C.close_index(C.int64_t(100))
	//C.hello_world_c(C.CString("foobar"))
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
	// TODO!
	return nil
}
