package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	ikvclient "github.com/pushkarmoi/ikv-store/ikv-go-client"
)

func TestGrpcHelloWorldCall(t *testing.T) {
	clientOptions, _ := ikvclient.NewClientOptionsBuilder().WithAccountId("testing-account-v1").WithAccountPasskey("testing-account-passkey").WithMountDirectory("/tmp/NearlineIntegrationTests").WithStoreName("testing-store").Build()
	writer, _ := ikvclient.NewDefaultIKVWriter(&clientOptions)

	writer.Startup()
	response, err := writer.Helloworld()
	if err != nil {
		errstring := err.Error()
		fmt.Println(errstring)
	}
	assert.Equal(t, err, nil)
	assert.Equal(t, response.Echo, "foo")
}

func TestHelloName(t *testing.T) {
	factory := ikvclient.IKVClientFactory{}

	// create client options
	clientOptions, err := ikvclient.NewClientOptionsBuilder().WithAccountId("testing-account-v1").WithAccountPasskey("testing-account-passkey").WithMountDirectory("/tmp/NearlineIntegrationTests").WithStoreName("testing-store").Build()
	assert.Equal(t, err, nil)

	// create reader
	reader, err := factory.CreateNewReader(&clientOptions)
	assert.Equal(t, err, nil)

	// create writer
	writer, err := factory.CreateNewWriter(&clientOptions)
	assert.Equal(t, err, nil)

	// startup
	err = reader.Startup()
	if err != nil {
		fmt.Println(err.Error())
	}
	assert.Equal(t, err, nil)
	assert.Equal(t, writer.Startup(), nil)

	// Create and upsert a document
	/*
		document, err := ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_1").PutStringField("firstname", "Alice").Build()
		assert.Equal(t, err, nil)
		err = writer.UpsertFields(&document)
		assert.Equal(t, err, nil)
		time.Sleep(10 * time.Second)
	*/

	// read field
	{
		value, _ := reader.GetStringValue("id_1", "userid")
		assert.Equal(t, value, "id_1")
	}
	{
		value, _ := reader.GetStringValue("id_1", "firstname")
		assert.Equal(t, value, "Alice")
	}

	// shutdown
	assert.Equal(t, reader.Shutdown(), nil)
	assert.Equal(t, writer.Shutdown(), nil)
}
