package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	ikvclient "github.com/inlinedio/ikv-store/ikv-go-client"
)

// Tests a GRPC hello-world call.
func TestGrpcHelloWorldCall(t *testing.T) {
	clientOptions, _ := ikvclient.NewClientOptionsBuilder().WithAccountId("foo").WithAccountPasskey("bar").WithStoreName("baz").Build()
	writer, _ := ikvclient.NewDefaultIKVWriter(&clientOptions)

	writer.Startup()
	response, err := writer.Helloworld("foo")
	if err != nil {
		errstring := err.Error()
		fmt.Println(errstring)
	}
	assert.Equal(t, err, nil)
	assert.Equal(t, response.Echo, "foo")
}

func TestSingleSetGet(t *testing.T) {
	t.Skip("ignore-test")

	accountid := "foo"
	accountpasskey := "bar"

	factory := ikvclient.IKVClientFactory{}

	// create client options
	clientOptions, err := ikvclient.NewClientOptionsBuilder().WithAccountId(accountid).WithAccountPasskey(accountpasskey).WithMountDirectory("/tmp/GoIntegTestStore").WithStoreName("testing-store").Build()
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
	*/

	//time.Sleep(10 * time.Second)

	// read fields 10M times
	for i := 0; i < 10000000; i++ {
		{
			value, _ := reader.GetStringValue("id_1", "userid")
			assert.Equal(t, value, "id_1")
		}
		{
			value, _ := reader.GetStringValue("id_1", "firstname")
			assert.Equal(t, value, "Alice")
		}
	}

	// shutdown
	assert.Equal(t, reader.Shutdown(), nil)
	assert.Equal(t, writer.Shutdown(), nil)
}

func TestUpsertAndDelete(t *testing.T) {
	t.Skip("ignore-test")

	accountid := "foo"
	accountpasskey := "bar"

	// initialize writer and reader
	factory := ikvclient.IKVClientFactory{}
	clientOptions, err := ikvclient.NewClientOptionsBuilder().WithAccountId(accountid).WithAccountPasskey(accountpasskey).WithMountDirectory("/tmp/GoIntegTestStore").WithStoreName("testing-store").Build()
	assert.Equal(t, err, nil)
	reader, err := factory.CreateNewReader(&clientOptions)
	assert.Equal(t, err, nil)
	writer, err := factory.CreateNewWriter(&clientOptions)
	assert.Equal(t, err, nil)
	assert.Equal(t, reader.Startup(), nil)
	assert.Equal(t, writer.Startup(), nil)

	// upsert {"userid": "id_1", "firstname": "Alice"}
	document, err := ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_1").PutStringField("firstname", "Alice").Build()
	assert.Equal(t, err, nil)
	err = writer.UpsertFields(&document)
	assert.Equal(t, err, nil)

	time.Sleep(5 * time.Second)

	// read firstname
	value, _ := reader.GetStringValue("id_1", "firstname")
	assert.Equal(t, value, "Alice")

	// delete field "firstname"
	docid, _ := ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_1").Build()
	assert.Equal(t, writer.DeleteFields(&docid, []string{"firstname"}), nil)

	time.Sleep(5 * time.Second)

	// read firstname, should be empty, userid is present
	value, _ = reader.GetStringValue("id_1", "firstname")
	assert.Equal(t, value, "")
	value, _ = reader.GetStringValue("id_1", "userid")
	assert.Equal(t, value, "id_1")

	docid, _ = ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_1").Build()
	assert.Equal(t, writer.DeleteDocument(&docid), nil)

	time.Sleep(5 * time.Second)

	// deleted doc, both empty
	value, _ = reader.GetStringValue("id_1", "firstname")
	assert.Equal(t, value, "")
	value, _ = reader.GetStringValue("id_1", "userid")
	assert.Equal(t, value, "")
}
