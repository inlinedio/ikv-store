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
	t.Skip("ignore-test")

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

	document, err := ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_1").PutStringField("firstname", "Alice").Build()
	assert.Equal(t, err, nil)
	err = writer.UpsertFields(&document)
	assert.Equal(t, err, nil)

	time.Sleep(10 * time.Second)

	// read fields 10M times
	for i := 0; i < 10000000; i++ {
		{
			_, value, _ := reader.GetStringValue("id_1", "userid")
			assert.Equal(t, value, "id_1")
		}
		{
			_, value, _ := reader.GetStringValue("id_1", "firstname")
			assert.Equal(t, value, "Alice")
		}
	}

	// shutdown
	assert.Equal(t, reader.Shutdown(), nil)
	assert.Equal(t, writer.Shutdown(), nil)
}

func TestMultiGet(t *testing.T) {
	t.Skip("ignore-test")

	accountid := "foo"
	accountpasskey := "bar"

	// initialize writer and reader
	factory := ikvclient.IKVClientFactory{}
	clientOptions, _ := ikvclient.NewClientOptionsBuilder().WithAccountId(accountid).WithAccountPasskey(accountpasskey).WithMountDirectory("/tmp/GoIntegTestStore").WithStoreName("testing-store").Build()
	reader, _ := factory.CreateNewReader(&clientOptions)
	defer reader.Shutdown()
	writer, _ := factory.CreateNewWriter(&clientOptions)
	defer writer.Shutdown()
	assert.Equal(t, reader.Startup(), nil)
	assert.Equal(t, writer.Startup(), nil)

	// upsert {"userid": "id_0", "firstname": "Alice"}
	// upsert {"userid": "id_1", "firstname": "Bob"}
	// upsert {"userid": "id_2"}
	doc0, _ := ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_0").PutStringField("firstname", "Alice").Build()
	doc1, _ := ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_1").PutStringField("firstname", "Bob").Build()
	doc2, _ := ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_2").Build()

	// delete to clear
	if err := writer.DeleteDocument(&doc0); err != nil {
		assert.Equal(t, err, nil)
	}
	if err := writer.DeleteDocument(&doc1); err != nil {
		assert.Equal(t, err, nil)
	}
	if err := writer.DeleteDocument(&doc2); err != nil {
		assert.Equal(t, err, nil)
	}

	// upsert
	if err := writer.UpsertFields(&doc0); err != nil {
		assert.Equal(t, err, nil)
	}
	if err := writer.UpsertFields(&doc1); err != nil {
		assert.Equal(t, err, nil)
	}
	if err := writer.UpsertFields(&doc2); err != nil {
		assert.Equal(t, err, nil)
	}

	time.Sleep(5 * time.Second)

	results, err := reader.MultiGetBytesValues([]interface{}{"id_1", "id_0", "foo", "id_2"}, []string{"firstname", "bar"})
	assert.Equal(t, err, nil)

	// reads
	assert.Equal(t, string(results[0]), "Bob")
	assert.Nil(t, results[1])
	assert.Equal(t, string(results[2]), "Alice")
	assert.Nil(t, results[3])
	// invalid doc
	assert.Nil(t, results[4])
	assert.Nil(t, results[5])
	// both fields don't exist
	assert.Nil(t, results[6])
	assert.Nil(t, results[7])

	// no fields - empty results
	results, _ = reader.MultiGetBytesValues([]interface{}{"id_1"}, []string{})
	assert.Equal(t, 0, len(results))

	_, err := reader.MultiGetBytesValues([]interface{}{"id_1", []byte(nil)}, []string{"firstname"})
	assert.NotNil(t, err, nil)
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
	_, value, _ := reader.GetStringValue("id_1", "firstname")
	assert.Equal(t, value, "Alice")

	// delete field "firstname"
	docid, _ := ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_1").Build()
	assert.Equal(t, writer.DeleteFields(&docid, []string{"firstname"}), nil)

	time.Sleep(5 * time.Second)

	// read firstname, should be empty, userid is present
	exists, value, _ := reader.GetStringValue("id_1", "firstname")
	assert.Equal(t, exists, false)
	exists, value, _ = reader.GetStringValue("id_1", "userid")
	assert.Equal(t, exists, true)
	assert.Equal(t, value, "id_1")

	docid, _ = ikvclient.NewIKVDocumentBuilder().PutStringField("userid", "id_1").Build()
	assert.Equal(t, writer.DeleteDocument(&docid), nil)

	time.Sleep(5 * time.Second)

	// deleted doc, both empty
	exists, value, _ = reader.GetStringValue("id_1", "firstname")
	assert.Equal(t, exists, false)
	exists, value, _ = reader.GetStringValue("id_1", "userid")
	assert.Equal(t, exists, false)
}

func TestUpsertAndDropField(t *testing.T) {
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
	_, value, _ := reader.GetStringValue("id_1", "firstname")
	assert.Equal(t, value, "Alice")

	// drop field "firstname", and no-op attempt to delete primary-key
	err = writer.DropFieldsByName([]string{"firstname", "userid"})
	assert.Equal(t, err, nil)

	time.Sleep(5 * time.Second)

	// read firstname, should be empty, userid is present
	exists, value, _ := reader.GetStringValue("id_1", "firstname")
	assert.Equal(t, exists, false)
	exists, value, _ = reader.GetStringValue("id_1", "userid")
	assert.Equal(t, exists, true)
	assert.Equal(t, value, "id_1")
}
