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

func example(t *testing.T) error {
	factory := ikvclient.IKVClientFactory{}

	// create client options - for writer
	writerClientOptions, err := ikvclient.NewClientOptionsBuilder().WithStoreName("--store-name--").WithAccountId("--account-id").WithAccountPasskey("--account-passkey").Build()
	if err != nil {
		return err
	}

	// create and startup writer
	writer, err := factory.CreateNewWriter(&writerClientOptions)
	if err != nil {
		return err
	}

	if err = writer.Startup(); err != nil {
		return err
	}

	// create documents and invoke upsert() operations
	doc1, err := ikvclient.NewIKVDocumentBuilder().PutStringField("firstname", "Alice").PutStringField("age", "22").Build()
	if err != nil {
		return err
	}
	if err = writer.UpsertFields(&doc1); err != nil {
		return err
	}

	doc2, err := ikvclient.NewIKVDocumentBuilder().PutStringField("firstname", "Alice").PutStringField("city", "San Francisco").Build()
	if err != nil {
		return err
	}
	if err = writer.UpsertFields(&doc2); err != nil {
		return err
	}

	doc3, err := ikvclient.NewIKVDocumentBuilder().PutStringField("firstname", "Bob").PutStringField("age", "25").Build()
	if err != nil {
		return err
	}
	if err = writer.UpsertFields(&doc3); err != nil {
		return err
	}

	// create client options - for reader
	readerClientOptions, err := ikvclient.NewClientOptionsBuilder().WithMountDirectory("/tmp/UserProfiles").WithStoreName("--store-name--").WithAccountId("--account-id").WithAccountPasskey("--account-passkey").Build()
	if err != nil {
		return err
	}

	// create and startup reader
	reader, err := factory.CreateNewReader(&readerClientOptions)
	if err != nil {
		return err
	}
	if err = reader.Startup(); err != nil {
		return err
	}

	// read documents
	// Due to eventual-consistent nature of IKV, you might need to add a small delay (or retries)
	// before reading your writes.
	time.Sleep(1 * time.Second)

	if value, err := reader.GetStringValue("Alice", "firstname"); err != nil {
		assert.Equal(t, value, "Alice")
	}
	if value, err := reader.GetStringValue("Alice", "age"); err != nil {
		assert.Equal(t, value, "22")
	}
	if value, err := reader.GetStringValue("Alice", "city"); err != nil {
		assert.Equal(t, value, "San Francisco")
	}

	if value, err := reader.GetStringValue("Bob", "firstname"); err != nil {
		assert.Equal(t, value, "Bob")
	}
	if value, err := reader.GetStringValue("Bob", "age"); err != nil {
		assert.Equal(t, value, "25")
	}
	if value, err := reader.GetStringValue("Bob", "city"); err != nil {
		// missing: zero-value
		assert.Equal(t, value, "")
	}

	// read fields
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

	return nil
}
