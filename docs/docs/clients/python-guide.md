---
title: Python
label: Python
sidebar_position: 1
---

# Python Guide

In this section we go over how to use IKV's Python SDK.

 - [Installation](#installation)
 - [Quick Start Example](#quick-start-example)
 - [Write Documents](#write-documents)
 - [Read Documents](#read-documents)
 - [Configuration](#configuration)

### Installation
Fetch the latest version using the **pip** tool.

```bash
python -m pip install ikvpy
```

### Quick Start Example
We will illustrate writing and reading "user profiles" in IKV using it's Go client. Each user-profile
can consist of the following fields: a string "firstname", a string "age" and a string "city". We demonstrate
creating documents, instantiating writer/reader clients and then writing/reading documents based on their primary-keys.

```python
import time
from ikvpy.clientoptions import ClientOptions, ClientOptionsBuilder
import ikvpy.client as ikv_client
import ikvpy.document as ikv_document
import ikvpy.factory as ikv_factory

# create client options - for writer
client_options = ClientOptionsBuilder() \
	.with_account_id("--account-id--")\
	.with_account_passkey("--account-passkey--")\
	.with_store_name("--store-name--")\
	.build()

# create writer
writer: ikv_client.IKVWriter = ikv_factory.create_new_writer(client_options)
writer.startup()

# create documents and invoke upsert() operations
doc1 = ikv_document.IKVDocumentBuilder().put_string_field("firstname", "Alice").put_string_field("age", "22").build()
writer.upsert_fields(doc1) # can raise exception on error, see source docs

doc2 = ikv_document.IKVDocumentBuilder().put_string_field("firstname", "Alice").put_string_field("city", "San Francisco").build()
writer.upsert_fields(doc2) # can raise exception on error, see source docs

doc3 = ikv_document.IKVDocumentBuilder().put_string_field("firstname", "Bob").put_string_field("age", "25").build()
writer.upsert_fields(doc3) # can raise exception on error, see source docs

# create client options - for reader
client_options = ClientOptionsBuilder() \
	.with_account_id("--account-id--")\
	.with_account_passkey("--account-passkey--")\
	.with_store_name("--store-name--")\
	.with_mount_directory("--mount-directory--")\
	.build()

# create reader
reader: ikv_client.IKVReader = ikv_factory.create_new_reader(client_options)
reader.startup()

# read documents

# Due to eventual-consistent nature of IKV, you might need to add a small delay (or retries)
# before reading your writes.
time.Sleep(1)

# single gets
assert reader.get_string_value("Alice", "firstname") == "Alice"
assert reader.get_string_value("Alice", "age") is None
assert reader.get_string_value("Alice", "city") == "San Francisco"
assert reader.get_string_value("Bob", "firstname") == "Bob"
assert reader.get_string_value("Bob", "age") == "25"
assert reader.get_string_value("Bob", "city") is None

# multi/batch get
cities = reader.multiget_string_values(str_primary_keys=["Alice", "Bob"])

# note: the returned iterator can be drained into a list using list(cities)
assert next(cities) == "San Francisco"
assert next(cities) is None
```

### Write Documents
You will interact with the following main interfaces to write (insert/update/delete) documents in an IKV store:

1. `IKVDocument` - A document is a collection of fields/attributes, identified by a primary-key. It is an abstraction for modeling data and writing/reading from IKV.
2. `IKVWriter` - A writer client which can insert/update documents i.e. `IKVDocument` structs, to a particular IKV store.
3. Factory method `create_new_writer` for creating an `IKVWriter` instance, by accepting various configuration options i.e. `ClientOptions` struct ([reference](#configuration)).

#### Creating Documents
`IKVDocument` objects are created by instantiating an `IKVDocumentBuilder` object, and then invoking various `put_...()` methods to add field values, and finally invoking `build()`. A document must always contain the field-value for it's primary-key, and it's partitioning-key (if it was created during store provisioning). The following types of fields can be added to a document:
 - string via put_string_field(): Provide a string field-name and `str` field-value
 - bytes via put_string_field(): Provide a string field-name and `bytes` field-value
 - numeric fields (int64/float64/etc.): Not available yet.

#### Write Operations via IKVWriter
-  **startup**: Initialize the instance.
-  **shutdown**: Terminate the instance.
-  **upsert_fields**: Insert or Update (if not exists) fields for a document. The document must contain the value of the primary-key and (if-applicable) partitioning-key. Include values of other fields you wish to upsert. Different values for the same primary-key are aggregated by unionizing distincts and overwriting duplicates, ex. upsert of `{"name": "Alice", "age": 22}` followed by upsert of `{"name": "Alice", "age": 25, "city": "San Francisco"}` , results in `{"name": "Alice", "age": 25, "city": "San Francisco"}` being saved in IKV.
-  **delete_fields**: Delete specified fields from a document, if they exist. The document must contain the value of the primary-key and (if-applicable) partitioning-key.
-  **delete_document**: Deletes specified document if it exists. The document must contain the value of the primary-key and (if-applicable) partitioning-key.
-  **drop_fields_by_name**: Drop specified fields for all documents. Attempts to drop primary-key field are silently ignored (no error).
-  **drop_fields_by_name_prefix**: Drop specified fields for all documents, by specifying field name prefixes. Attempts to drop primary-key field are silently ignored (no error).
-  **drop_all_fields**: Drop all documents from this store.

:::info
**Multithreaded Usage**: All write operations on IKVWriter are thread-safe. However, you must ensure there is caller side synchronization while
invoking write operations for documents with the same primary-key with multiple threads, to avoid unintended results.
:::

### Read Documents
You will interact with the following main interfaces to read document fields from an IKV store:

1. `IKVReader` - A reader client over an embedded key-value store, which can be used to read strongly-typed field-values for a given document.
2. Factory method `create_new_reader` for creating an `IKVReader` instance, by accepting various configuration options for the embedded key-value store i.e. `ClientOptions` struct ([reference](#configuration)).

#### Read Operations via IKVReader
-  **startup**: Initialize the embedded key-value store. This operation can block to pull latest data to your local instance.
-  **shutdown**: Terminate the embedded key-value store.
-  **get_bytes_value**: Returns the `bytes` field-value for a primary-key if it exists, else `None`.
-  **get_string_value**: Returns the `string` field-value for a primary-key if it exists, else `None`.
-  **multiget_bytes_values**: Batch-get method to retrieve field-values for provided primary-keys in the form of a `bytes` iterator. The iterator returns values in order, and `None` if a value does not exist for the corresponding key.
-  **multiget_string_values**: Batch-get method to retrieve field-values for provided primary-keys in the form of a `string` iterator. The iterator returns values in order, and `None` if a value does not exist for the corresponding key.

:::info
**Multithreaded Usage**: All read operations on IKVReader are thread-safe.
:::

### Configuration
`ClientOptions` provide configuration for instantiating an IKVReader/IKVWriter instance. It is instantiated by using a `ClientOptionsBuilder` object, invoking various named methods like `with_store_name()` and finally invoking `build()`. Summary of writer client configuration options -

| Option            	| Method Name        	| Description                                                                                                                                                                                                                                                          	|
|-------------------	|--------------------	|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| store_name        	| with_store_name      	| Always required for IKVReader/IKVWriter, case-sensitive string. The IKV store name                                                                                                                                                                                   	|
| accound_id        	| with_account_id      	| Always required for IKVReader/IKVWriter, case-sensitive string. Provisioned IKV account-id                                                                                                                                                                           	|
| account_passkey   	| with_account_passkey 	| Always required for IKVReader/IKVWriter, case-sensitive string. Provisioned IKV account-passkey                                                                                                                                                                      	|
| mount_directory   	| with_mount_directory 	| Required only for IKVReader, case-sensitive string. The absolute path to use as the mount point for this IKV store.  This mount point will be used to store the IKV embedded key-value  store locally on this machine.                                               	|
| console_log_level 	| with_console_logging 	| Optional for IKVReader/IKVWriter, case-sensitive string. Enables logging to stdout.  Set log level for log filtering, can be one of: "ERROR", "WARN", "INFO", "DEBUG", "TRACE"                                                                                       	|
| file_log_level    	| with_file_logging    	| Optional for IKVReader/IKVWriter, case-sensitive strings. Enables logging to specified file path. Set log level for log filtering, can be one of: "ERROR", "WARN", "INFO", "DEBUG", "TRACE"  If this method is not used, console logging will be used at INFO level. 	|