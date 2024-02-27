package main

import (
	"fmt"

	ikv "github.com/inlinedio/ikv-store/ikv-go-client"
)

func main() {
	bm, err := ikv.NewBinaryManager("/tmp/cmd-main-bin")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	dllpath, err := bm.GetPathToNativeBinary()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	nr, err := ikv.NewNativeReaderV2(dllpath)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	status, err := nr.HealthCheck("hello-world")
	fmt.Println("Status Code: ", status)
	fmt.Println("Status Error: ", err)
}
