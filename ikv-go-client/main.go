package main

/*
#include <stdlib.h>
#cgo LDFLAGS: -L/Users/pushkar/projects/ikv-store/ikv/target/release -likv
#include "/Users/pushkar/projects/ikv-store/ikv/src/ffi/c_api.h"
*/
import "C"

func main() {
	// An example for illustrating Go->Rust interaction using cgo.
	C.close(C.longlong(int64(0)))
	C.hello_world(C.CString("foobar"))
}
