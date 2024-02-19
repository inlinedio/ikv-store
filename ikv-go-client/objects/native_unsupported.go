//go:build !cgo || !(linux || darwin) || !(amd64 || arm64)

package objects

import (
	"errors"
	"fmt"
)

func PrintHelloWorld(input string) {
	fmt.Println("Platform not supported. Supported - linux or darwin on x86 or arm64; with CGO enabled")
}

func HealthCheck(input string) (bool, error) {
	return false, errors.New("platform not supported. Supported - linux or darwin on x86 or arm64; with CGO enabled")
}

func Open(config []byte) (int64, error) {
	return 0, errors.New("platform not supported. Supported - linux or darwin on x86 or arm64; with CGO enabled")
}

func Close(handle int64) error {
	return errors.New("platform not supported. Supported - linux or darwin on x86 or arm64; with CGO enabled")
}

func GetFieldValue(handle int64, primaryKey []byte, fieldName string) []byte {
	return make([]byte, 0)
}
