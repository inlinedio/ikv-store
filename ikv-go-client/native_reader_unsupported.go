//go:build !cgo

package ikvclient

import (
	"errors"
)

type NativeReaderV2 struct {
}

func NewNativeReaderV2(dllPath string) (*NativeReaderV2, error) {
	nr := &NativeReaderV2{}
	return nr, nil
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
