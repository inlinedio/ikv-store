---
title: Java
label: Java
sidebar_position: 1
---

# Java Usage Guide
In this section we go over how to use IKV's Java client library.

 - [Installation](#installation)
 - [Example](#example)
 - [Write Documents](#write-documents)
 - [Read Documents](#read-documents)
 - [Configuration](#configuration)

### Installation
`ikv-java-client` is hosted on [Jitpack](https://jitpack.io/#io.inlined/ikv-java-client). The latest version is: [![Release](https://jitpack.io/v/io.inlined/ikv-java-client.svg)](https://jitpack.io/#io.inlined/ikv-java-client)

#### Gradle
```groovy
repositories {
  maven { url 'https://jitpack.io' }
}

dependencies {
  implementation 'io.inlined:ikv-java-client:0.0.7'
}
```

#### Maven
```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
  <groupId>io.inlined</groupId>
  <artifactId>ikv-java-client</artifactId>
  <version>0.0.7</version>
</dependency>
```

### Example
We will illustrate writing and reading "user profiles" in IKV using it's Java client. Each user-profile
can consist of the following fields: a string "firstname", an integer "age" and a string "city". We demonstrate
creating documents, instantiating writer/reader clients and then writing/reading documents based on their primary-keys.

```java
import io.inlined.clients.ClientOptions;  
import io.inlined.clients.IKVClientFactory;  
import io.inlined.clients.IKVDocument;  
import io.inlined.clients.InlineKVReader;  
import io.inlined.clients.InlineKVWriter;

IKVClientFactory factory = new IKVClientFactory();

// Create client options - for Writer
ClientOptions writerClientOptions = new ClientOptions.Builder()
  .withStoreName("--store-name--")  
  .withAccountId("--account-id--")  
  .withAccountPassKey("--account-passkey--")
  .build();

// Create Writer instance
InlineKVWriter writer = factory.createNewWriterInstance(writerClientOptions);
writer.startupWriter();

// create documents and invoke upsert() operations
IKVDocument doc1 =  new IKVDocument.Builder()
  .putStringField("firstname", "Alice")
  .putIntField("age", 22)
  .build();  
writer.upsertFieldValues(doc1);

IKVDocument doc2 =  new IKVDocument.Builder()
  .putStringField("firstname", "Alice")
  .putStringField("city", "San Francisco")
  .build();  
writer.upsertFieldValues(doc2);

IKVDocument doc3 =  new IKVDocument.Builder()
  .putStringField("firstname", "Bob")
  .putIntField("age", 25)
  .build();  
writer.upsertFieldValues(doc3);

// Create client options - for Reader
ClientOptions readerClientOptions = new ClientOptions.Builder()
  .withMountDirectory("/tmp/UserProfiles")  
  .withStoreName("--store-name--")  
  .withAccountId("--account-id--")  
  .withAccountPassKey("--account-passkey--")  
  .useStringPrimaryKey()  
  .build();

// Create Reader instance
InlineKVReader reader = factory.createNewReaderInstance(readerClientOptions);  
reader.startupReader();

// read documents
// Due to eventual-consistent nature of IKV, you might need to add a small delay (or retries)
// before reading your writes.
Thread.sleep(1000);

Assertions.assertEquals(reader.getStringValue("Alice", "firstname"), "Alice");
Assertions.assertEquals(reader.getIntValue("Alice", "age"), 22);
Assertions.assertEquals(reader.getStringValue("Alice", "city"), "San Francisco");

Assertions.assertEquals(reader.getStringValue("Bob", "firstname"), "Bob");
Assertions.assertEquals(reader.getIntValue("Bob", "age"), 25);
Assertions.assertNull(reader.getStringValue("Bob", "city"));

writer.shutdownWriter();
reader.shutdownReader();
```

### Write Documents
This section provides more details about writing data to IKV. You will interact with the following main components to write (insert/update/delete) documents in an IKV store:

1. `IKVDocument` - A document is a collection of fields/attributes, identified by a primary-key. It is an abstraction for modeling data and writing/reading from IKV.
2. `InlineKVWriter` - A writer client which can insert/update/delete documents i.e. `IKVDocument` objects, to a particular IKV store.
3. `IKVClientFactory` - A factory class for creating an `InlineKVWriter` instance, by accepting various configuration options i.e. `ClientOptions` object ([reference](#configuration)).

#### Creating Documents
`IKVDocument` objects are created by using an `IKVDocument.Builder` object, and then invoking various `put...()` methods and finally invoking `build()`. A document must always contain the field-value for it's primary-key, and it's partitioning-key (if it was created during store provisioning). The following types of fields can be added to a document:

 - string via putStringField(): Provide a string field-name and string field-value
 - bytes via putBytesField(): Provide a string field-name and byte[] field-value
 - int/long/float/double: Provide a string field-name and int/long/float/double field-value to the appropriate `put...()` method.

#### Write Operations via InlineKVWriter
-  **startupWriter**: Initialize the instance.
-  **shutdownWriter**: Terminate the instance.
-  **upsertFieldValues**: Insert or Update (if not exists) fields for a document. The document must contain the value of the primary-key and (if-specified) partitioning-key. Include values of other fields you wish to upsert. Different values for the same primary-key are aggregated by unionizing distincts and overwriting duplicates, ex. upsert of `{"name": "Alice", "age": 22}` followed by upsert of `{"name": "Alice", "age": 25, "city": "San Francisco"}` , results in `{"name": "Alice", "age": 25, "city": "San Francisco"}` being saved in IKV.
-  **deleteFieldValues**: Delete specified fields from a document, if they exist. The document must contain the value of the primary-key and (if-specified) partitioning-key.
-  **deleteDocument**: Deletes specified document if it exists. The document must contain the value of the primary-key and (if-specified) partitioning-key.

:::info
**Multithreaded Usage**: All write operations on InlineKVWriter are thread-safe. However, you must ensure there is caller side synchronization while
invoking write operations for documents with the same primary-key with multiple threads, to avoid unintended results.
:::

### Read Documents
This section provides more details about reading data from IKV. You will interact with the following main components to read document fields from an IKV store:

1. `InlineKVReader` - A read client over an embedded key-value store, which can be used to read strongly-typed field-values for a given document.
2. `IKVClientFactory` - A factory class for creating an `InlineKVReader` instance, by accepting various configuration options for the embedded key-value store i.e. `ClientOptions` object ([reference](#configuration)).

#### Read Operations via InlineKVReader
-  **startupReader**: Initialize the embedded key-value store. This operation can block to pull latest data to your local instance.
-  **shutdownReader**: Terminate the embedded key-value store.
-  **getBytesValue**: Read a byte[] field-value, given a primary-key. Null if the field or document is not present.
-  **multiGetBytesValues**: Batch lookup version of **getBytesValue**.
-  **getStringValue**: Read a String field-value, given a primary-key. Null if the field or document is not present.
-  **multiGetStringValues**: Batch lookup version of **getStringValue**.
-  **getIntValue**: Read an Integer field-value, given a primary-key. Null if the field or document is not present.
-  **getLongValue**: Read a Long field-value, given a primary-key. Null if the field or document is not present.
-  **getFloatValue**: Read a Float field-value, given a primary-key. Null if the field or document is not present.
-  **getDoubleValue**: Read a Double field-value, given a primary-key. Null if the field or document is not present.

:::info
**Multithreaded Usage**: All read operations on InlineKVReader are thread-safe.
:::


### Configuration
`ClientOptions` provide configuration for instantiating an InlineKVReader/InlineKVWriter instance. It is created using a `ClientOptions.Builder` object, invoking various named methods like `withStoreName()` and finally invoking `build()`. Summary of writer client configuration options -

| Option            	| Method Name                              	| Description                                                                                                                                                                                                                                                                    	|
|-------------------	|------------------------------------------	|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| store_name        	| withStoreName                            	| Always required for InlineKVReader/InlineKVWriter, case-sensitive string. The IKV store name                                                                                                                                                                                   	|
| accound_id        	| withAccountId                            	| Always required for InlineKVReader/InlineKVWriter, case-sensitive string. Provisioned IKV account-id                                                                                                                                                                           	|
| account_passkey   	| withAccountPassKey                       	| Always required for InlineKVReader/InlineKVWriter, case-sensitive string. Provisioned IKV account-passkey                                                                                                                                                                      	|
| mount_directory   	| withMountDirectory                       	| Required only for InlineKVReader, case-sensitive string. The absolute path to use as the mount point for this IKV store.  This mount point will be used to store the IKV embedded key-value  store locally on this machine.                                                    	|
| primary_key       	| useStringPrimaryKey,  useBytesPrimaryKey 	| Required only for InlineKVReader, use exactly one method. Specify the data type of primary-key for this IKV store (string or bytes).                                                                                                                                           	|
| console_log_level 	| withConsoleLogging                       	| Optional for InlineKVReader/InlineKVWriter, case-sensitive string. Enables logging to stdout.  Set log level for log filtering, can be one of: "ERROR", "WARN", "INFO", "DEBUG", "TRACE"                                                                                       	|
| file_log_level    	| withFileLogging                          	| Optional for InlineKVReader/InlineKVWriter, case-sensitive strings. Enables logging to specified file path. Set log level for log filtering, can be one of: "ERROR", "WARN", "INFO", "DEBUG", "TRACE"  If this method is not used, console logging will be used at INFO level. 	|










