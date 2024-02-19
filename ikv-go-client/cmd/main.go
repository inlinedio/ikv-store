package main

import (
	"fmt"

	"github.com/inlinedio/ikv-store/ikv-go-client/objects"
)

func main() {
	status, _ := objects.HealthCheck("foo")
	fmt.Println("Success: ", status)
}
