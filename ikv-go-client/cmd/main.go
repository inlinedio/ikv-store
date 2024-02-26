package main

import (
	"fmt"

	ikv "github.com/inlinedio/ikv-store/ikv-go-client"
)

func main() {
	nr, _ := ikv.NewNativeReaderV2("/Users/pushkar/libikv.dylib")
	status, err := nr.HealthCheck("hello-world")
	fmt.Println("Status Code: ", status)
	fmt.Println("Status Error: ", err)
}
