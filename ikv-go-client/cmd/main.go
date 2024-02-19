package main

import (
	"fmt"

	"github.com/inlinedio/ikv-store/ikv-go-client/objects"
)

func main() {
	status, err := objects.HealthCheck("foo")
	if err != nil {
		fmt.Println("Error: ", err)
	}
	fmt.Println("Status Code: ", status)
}
