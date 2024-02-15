package ikvclient

/*
#cgo LDFLAGS: -L/Users/pushkar/projects/ikv-store/ikv/target/release -likv
void hello_world(const char *input);
//void close(long handle);
*/
import "C"

func PrintHelloWorld() {
	C.hello_world(C.CString("foobar"))
}

type NativeReader struct {
}

func (nr *NativeReader) open(config []byte) (int64, error) {
	// TODO!
	return 0, nil
}

func (nr *NativeReader) close(handle int64) error {
	// TODO!
	return nil
}

func (nr *NativeReader) getFieldValue(handle int64, primaryKey []byte, fieldName string) []byte {
	// TODO!
	return nil
}
