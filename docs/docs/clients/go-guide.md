---
title: Go
label: Go
sidebar_position: 2
---

# Go Guide
In this section we go over how to use IKV's Go SDK.

 - [Installation](#installation)
 - [Example](#example)
 - [Write Documents](#write-documents)
 - [Read Documents](#read-documents)
 - [Configuration](#configuration)

### Installation
Fetch the latest version using the **go** tool.

```bash
go get github.com/inlinedio/ikv-store@latest
```
```go
import (
  ikvclient "github.com/inlinedio/ikv-store/ikv-go-client"
)
```

### Example
We will illustrate writing and reading "user profiles" in IKV using it's Go client. Each user-profile
can consist of the following fields: a string "firstname", a string "age" and a string "city". We demonstrate
creating documents, instantiating writer/reader clients and then writing/reading documents based on their primary-keys.

```go
import (
  "fmt"
  "testing"
  "time"

  "github.com/stretchr/testify/assert"

  ikvclient "github.com/inlinedio/ikv-store/ikv-go-client"
)

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
  defer writer.Shutdown()

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
  defer reader.Shutdown()

  // read documents
  // Due to eventual-consistent nature of IKV, you might need to add a small delay (or retries)
  // before reading your writes.
  time.Sleep(1 * time.Second)

  if exists, value, err := reader.GetStringValue("Alice", "firstname"); err != nil {
    assert.Equal(t, exists, true)
    assert.Equal(t, value, "Alice")
  }
  if _, value, err := reader.GetStringValue("Alice", "age"); err != nil {
    assert.Equal(t, value, "22")
  }
  if _, value, err := reader.GetStringValue("Alice", "city"); err != nil {
    assert.Equal(t, value, "San Francisco")
  }

  if _, value, err := reader.GetStringValue("Bob", "firstname"); err != nil {
    assert.Equal(t, value, "Bob")
  }
  if _, value, err := reader.GetStringValue("Bob", "age"); err != nil {
    assert.Equal(t, value, "25")
  }
  if _, value, err := reader.GetStringValue("Bob", "city"); err != nil {
    // missing: zero-value
    assert.Equal(t, exists, false)
    assert.Equal(t, value, "")
  }

  return nil
}
```

### Write Documents
You will interact with the following main interfaces to write (insert/update/delete) documents in an IKV store:

1. `IKVDocument` - A document is a collection of fields/attributes, identified by a primary-key. It is an abstraction for modeling data and writing/reading from IKV.
2. `IKVWriter` - A writer client which can insert/update documents i.e. `IKVDocument` structs, to a particular IKV store.
3. `IKVClientFactory` - A factory struct for creating an `IKVWriter` instance, by accepting various configuration options i.e. `ClientOptions` struct ([reference](#configuration)).

#### Creating Documents
`IKVDocument` structs are created by instantiating an `IKVDocumentBuilder` struct with `NewIKVDocumentBuilder()` function, and then invoking various `Put...()` methods and finally invoking `Build()`. A document must always contain the field-value for it's primary-key, and it's partitioning-key (if it was created during store provisioning). The following types of fields can be added to a document:
 - string via PutStringField(): Provide a string field-name and string field-value
 - bytes via PutBytesField(): Provide a string field-name and []byte field-value
 - numeric fields (int64/float64/etc.): Not available yet.

#### Write Operations via IKVWriter
-  **Startup**: Initialize the instance.
-  **Shutdown**: Terminate the instance.
-  **UpsertFields**: Insert or Update (if not exists) fields for a document. The document must contain the value of the primary-key and (if-specified) partitioning-key. Include values of other fields you wish to upsert. Different values for the same primary-key are aggregated by unionizing distincts and overwriting duplicates, ex. upsert of `{"name": "Alice", "age": 22}` followed by upsert of `{"name": "Alice", "age": 25, "city": "San Francisco"}` , results in `{"name": "Alice", "age": 25, "city": "San Francisco"}` being saved in IKV.
-  **DeleteFields**: Delete specified fields from a document, if they exist. The document must contain the value of the primary-key and (if-specified) partitioning-key.
-  **DeleteDocument**: Deletes specified document if it exists. The document must contain the value of the primary-key and (if-specified) partitioning-key.
-  **DropFieldsByName**: Drop specified fields for all documents. Attempts to drop primary-key field are silently ignored (no error).
-  **DropFieldsByNamePrefix**: Drop specified fields for all documents, by specifying field name prefixes. Attempts to drop primary-key field are silently ignored (no error).
-  **DropAllFields**: Drop all documents from the store.

:::info
**Multithreaded Usage**: All write operations on IKVWriter are thread-safe. However, you must ensure there is caller side synchronization while
invoking write operations for documents with the same primary-key with multiple threads, to avoid unintended results.
:::

### Read Documents
You will interact with the following main interfaces to read document fields from an IKV store:

1. `IKVReader` - A read client over an embedded key-value store, which can be used to read strongly-typed field-values for a given document.
2. `IKVClientFactory` - A factory struct for creating an `IKVReader` instance, by accepting various configuration options for the embedded key-value store i.e. `ClientOptions` struct ([reference](#configuration)).

#### Read Operations via IKVReader
-  **Startup**: Initialize the embedded key-value store. This operation can block to pull latest data to your local instance.
-  **Shutdown**: Terminate the embedded key-value store.
-  **GetBytesValue**: Read a byte[] field-value, given a primary-key.
-  **GetStringValue**: Read a string field-value, given a primary-key.

:::info
**Multithreaded Usage**: All read operations on IKVReader are thread-safe.
:::


### Configuration
`ClientOptions` provide configuration for instantiating an IKVReader/IKVWriter instance. It is instantiated by using a `ClientOptionsBuilder` struct with `NewClientOptionsBuilder()`, invoking various named methods like `withStoreName()` and finally invoking `Build()`. Summary of writer client configuration options -

| Option            	| Method Name        	| Description                                                                                                                                                                                                                                                          	|
|-------------------	|--------------------	|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| store_name        	| WithStoreName      	| Always required for IKVReader/IKVWriter, case-sensitive string. The IKV store name                                                                                                                                                                                   	|
| accound_id        	| WithAccountId      	| Always required for IKVReader/IKVWriter, case-sensitive string. Provisioned IKV account-id                                                                                                                                                                           	|
| account_passkey   	| WithAccountPasskey 	| Always required for IKVReader/IKVWriter, case-sensitive string. Provisioned IKV account-passkey                                                                                                                                                                      	|
| mount_directory   	| WithMountDirectory 	| Required only for IKVReader, case-sensitive string. The absolute path to use as the mount point for this IKV store.  This mount point will be used to store the IKV embedded key-value  store locally on this machine.                                               	|
| console_log_level 	| WithConsoleLogging 	| Optional for IKVReader/IKVWriter, case-sensitive string. Enables logging to stdout.  Set log level for log filtering, can be one of: "ERROR", "WARN", "INFO", "DEBUG", "TRACE"                                                                                       	|
| file_log_level    	| WithFileLogging    	| Optional for IKVReader/IKVWriter, case-sensitive strings. Enables logging to specified file path. Set log level for log filtering, can be one of: "ERROR", "WARN", "INFO", "DEBUG", "TRACE"  If this method is not used, console logging will be used at INFO level. 	|