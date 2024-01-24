# IKV | Inlined.io Key Value Store
IKV is a **fully-managed [embedded database](https://en.wikipedia.org/wiki/Embedded_database)**, optimized for online feature serving for ML inference. It is ideal for building large scale distributed systems (ex. recommendation engines or information retrieval tasks) - which need low latency access to key-value data. IKV-Store is -

 1. **Blazing Fast**: With no network overhead, IKV offers P99 read latency of **1-100 microseconds** from client’s point-of-view (depending on key/value size and single/batch operations). This is orders of magnitude better (10x) than existing solutions like Redis.
 2. **Built for the Enterprise**: Extremely low operational overhead; pay-as-you-go with no provisioned hardware costs; Environment agnostic - use in the public cloud or on-prem.
 3. **Horizontally Scalable**: Handles large datasets (with partitions) and high read/write traffic (with replication). Built for streaming and batch ingestion of data.
 4. **Fully Persistent with Backup Data-Lake**: Write once and read forever. Read more about IKV's unique architecture which makes this possible for an embedded database.

## Quick Links
 - Getting Started
	 - [Provisioning](#provisioning)
	 - [Java](#getting-started-with-java)
	 - Python (upcoming - July 2024)
	 - Go (upcoming - July 2024)
 - [APIs](#apis)
 - [Architecture](#architecture)
 - [Benchmarks](#benchmarks)
 - [FAQs](#faq)
 - [Support](#support)
## Provisioning
IKV is a managed database solution, hence before creating a new store you need to request provisioning. This section is useful for anyone who does not have an account or wants to create a new store within an account. To provision (provisioning time is usually less than 12 hrs), reach out to to - ***onboarding[@]inlined.io***, with the following - 
 - Existing **account-id** (if exists, else mention you want a new account-id and account-passkey).
 - For store provisioning: name, primary key, partitioning key (optional), and estimated write volume (ex. avg key & value size in bytes and volume/QPS). 
 - Once you have an account-id, account-passkey and have provisioned a new store, you're all set to start using IKV.

Reach out to ***onboarding[@]inlined.io*** for any provisioning related questions or support.

## Getting Started with Java
In this section we go over some code samples about how to use IKV's client library in your Java project.
#### Installation
`ikv-java-client` is hosted with Github Packages, add dependency to your Gradle/Maven Java project. Make sure to use the latest version from [package list](https://github.com/inlinedio?tab=packages&repo_name=ikv-store).
```
repositories {
  maven {
    name = "GitHubPackages"  
    url = uri("https://maven.pkg.github.com/inlinedio/ikv-store")  
    credentials {
      // You can use the following public github tokens, or inject your own personal tokens.
      username = "inlinedio"  
      password = "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"  
    }
  }

 // ... other project repositories (ex. mavenCentral()) ..
}

dependencies {
  implementation group: 'io.inlined', name: 'ikv-java-client', version: '0.0.3'

  // .. other project dependencies ..
}
``` 
```
<dependency>
  <groupId>io.inlined</groupId>
  <artifactId>ikv-java-client</artifactId>
  <version>0.0.3</version>  
</dependency>
```
#### Java Usage
// todo
// refer to APIs section to understand interfaces
// 


## APIs
In this section we go over the key interfaces and abstractions that IKV provides as a key-value database. 

These concepts are language independent. IKV stores documents (the "value") associated with primary-keys (the "key"), in an eventually consistent (read-after-write manner). Given a primary-key, a user can do CRUD operations i.e. create/read/update/delete for a single document (batch operations are also supported).

##### Document
A document is a collection of fields/attributes. Each inner field/attribute contained within a document is optional (i.e. it's value can be missing), and can be used to store a particular property associated with a unit of data. This is analogous to a "single row" of data in many SQL databases or something like DynamoDB, with each "column" being an inner field/attribute. 

A field can be uniquely identified by a **name** and a **type** - which is the same across all documents stored in a particular IKV store. Fields (except primary and partitioning keys) are not required to be declared while provisioning or in a "schema/configuration" file, instead IKV updates field information dynamically as and when they are encountered at runtime.

##### Field Types
Supported types include - 
 - **Numeric**: int32, int64, float32, float64 - Can be used to store signed integer and floating point numbers of size 4 and 8 bytes.
 - **String**: A sequence of UTF8 characters. Language specific IKV clients generate functions/methods that use language specific types to represent strings (ex. `java.lang.String` in Java).
 - **Bytes**: A sequence of 8-bit values or raw bytes. This type is quite useful to model complex types. ex. 
	 - Storing a list of float64: which can be done by creating a custom encode/decoder which stores the number of float64's in the list followed by the actual floats as a fixed-width 8 byte entity.
	 - Storing complex/nested data: which can be done by leveraging serialization frameworks like Protobuf, Json, etc. - creating application specific objects and then converting them into raw bytes.

##### Primary Key
Primary Key is a unique identifier for a document and is itself a required field/attribute of the document. It can be of type string or bytes, and should be used for all CRUD operations on documents.

##### Partitioning Key
Since IKV is an embedded database, after a certain size all documents cannot fit in a single machine/host - hence partitioning keys are used to distribute documents across multiple hosts (i.e. partitions/shards). A user can declare an optional partitioning key (defaults to the primary key if missing), which itself is a required field/attribute of the document. It can be of type string or bytes. It is only relevant for write (create/update/delete) operations.

##### Operations
IKV supports the following operations on documents (for single/batch of documents) - 
 1. **Upsert**: Insert or Update-if-exists a document, given the document's fields (primary, partitioning keys and other fields). Upserts need not contain all field values as part of the same operation - fields get unionized to the same document, as and when they are published.
 2. **Delete**: Delete a document or specific fields from a document, given the document's primary and partitioning (if applicable) key.
 3. **Read**: Read a particular field(s) for a document, given its primary key. NOTE - Read operations are [eventually-consistent](https://en.wikipedia.org/wiki/Eventual_consistency) w.r.t upsert and delete operations, i.e. there will be small delay (usually in order of milliseconds) before changes to a document's fields are reflected in read operations.
 
##### Illustration
Let's consider an example where we want to store user profile's in an IKV store called - *users*. A user profile contains their (1) first-name (2) age (3) city.

To represent this data-model, we first think of how our IKV document would look like. Each document can represent  a particular profile, which is uniquely identified by their *firstname*. We will need the following types - 

 - Field-Name: firstname, Field-Type: String
 - Field-Name: age, Field-Type: int32
 - Field-Name: city, Field-Type: String

Document Construction:
```
Upsert#Alice
{
  "firstname": "Alice",
  "age": 22
}

Upsert#Alice
{
  "firstname": "Alice",
  "city": "San Francisco"
}

Upsert#Bob
{
  "firstname": "Bob",
  "age": 25
}
```

Reads:
```
"Alice#firstname" -> "Alice"
"Alice#age" -> 22
"Alice#city" -> "San Francisco"

"Bob#firstname" -> "Bob"
"Bob#age" -> 25
"Bob#city" -> // empty
```

## Architecture
// todo

## Benchmarks
We measure read **latency** from a Java client's point of view, while accessing InlineKV.
Latency measurements - we track latency at various percentiles to measure delay in execution time that client's can expect to see.

#### Benchmarking environment - 
1. Single threaded client JVM instance using InlineKV's Java client (see /ikv-java-client)
2. Hardware - AWS r5.xlarge instance (4 vcpu, 32GB).

#### Parameters - 
1. We use byte arrays as the format for key and value. Key is ~ 10 bytes in size, Value is 350 bytes.
2. Single: i.e. return value for a single key
3. Batch: return values for the specified batch of keys
   
#### Results - 
Instance type - r5.xlarge (4 vcpu, 32GB)
Key: 10 bytes, Value: 50 bytes

| Type   | Parameters                         | Latency (microseconds)                             |
|--------|------------------------------------|----------------------------------------------------|
| SINGLE | num_samples:100,000                | avg: 1.07, p50: 1.00, p90: 1.00, p99: 3.00         |
| BATCH  | num_samples:10,000, batch_size:10  | avg: 5.40, p50: 5.00, p90: 7.00, p99: 10.00        |
|        | num_samples:10,000, batch_size:50  | avg: 18.44, p50: 18.00, p90: 23.00, p99: 33.00     |
|        | num_samples:10,000, batch_size:100 | avg: 32.37, p50: 31.00, p90: 39.00, p99: 51.00     |
|        | num_samples:10,000, batch_size:500 | avg: 242.69, p50: 228.00, p90: 278.00, p99: 347.00 |

## FAQ
// todo

## Support
// todo

