---
sidebar_position: 3
---

# Data Modeling 
In this section we describe how to model your data for IKV stores.

## Glossary
- **Document**: Analogous to a row/object/entity of data in databases. Every IKV stores data in the form of documents.
- **Fields**: Analogous to columns in a row of data or attributes of an entity. A document is comprised of multiple strongly typed attributes known as fields.
- **Primary Key**: A field which can be used to uniquely identify a document. The field to be used as the primary-key is set upon IKV store provisioning and cannot be changed later.
- **Partitioning Key**: A field which can be used to segregate a document into different shards/partitions for the embedded key-value store. Using a partitioning key is optional, if done, it needs to be set upon IKV store provisioning and cannot be changed later.

## Document
A document in IKV is analogous to a row in SQL databases or an object/entity in NoSQL databases. It is a collection of stronly typed values (i.e. fields) belonging to the entity being represented. Documents can be uniquely identified using a primary-key (which is a field of the document itself). Documents are created, written to IKV (insert/update/delete) and then fields can be queried (primary-key and field-name), using language specific client libraries.

## Fields
Documents are comprised of fields. A field can be uniquely identified by a name (i.e. "field-name") and a type, which is the same across all documents stored in a particular IKV store. Document fields have the following properties:
 - Every field except primary-key and partitioning-key (if applicable) is optional in a document i.e. it may be missing when queried.
 - New fields (new name/type pair) is added as and when encountered - i.e. IKV documents do not have a "schema".
 - All documents store the same set of fields in a particular IKV store (with different values ofcourse).

#### Field Types
 - **Numeric**: int32, int64, float32, float64 - Can be used to store signed integer and floating point numbers of size 4 and 8 bytes.
 - **String**: A sequence of UTF8 characters. Language specific IKV clients generate functions/methods that use language specific types to represent strings (ex. `java.lang.String` in Java).
 - **Bytes**: A sequence of 8-bit values or raw bytes. This type is quite useful to model complex types. ex. 
	 - Storing a list of float64: which can be done by creating a custom encode/decoder which stores the number of float64's in the list followed by the actual floats as a fixed-width 8 byte entity.
	 - Storing nested data: which can be done by leveraging serialization frameworks like Protobuf, Json, etc. - creating application specific objects and then converting them into raw bytes.

## Query Pattern
IKV can be queried as a key-value store. The "key" for a query (read request) is the primary-key for a document, along with a field-name. The "value" is the field-value stored in the document. There is also support to perform batch/multi lookup operations.

## Example
Let's consider an example where we want to store "user profile" data in an IKV store called - *users*. A user profile contains their (1) first-name (2) age (3) city. To represent this data-model, we first think of how our IKV document would look like. Each document can represent a particular profile, which is uniquely identified by their *firstname*. We will need the following types - 
 - Field-Name: firstname, Field-Type: string
 - Field-Name: age, Field-Type: int32
 - Field-Name: city, Field-Type: string
We can create the following sets of documents to represent two users - "Alice" and "Bob".

```
Operation: Upsert#Alice
Document:
{
  "firstname": "Alice",
  "age": 22
}
```

At this stage, one document is indexed in the store, and two fields exist in each document i.e. "firstname" and "age"

```
Operation: Upsert#Alice
Document:
{
  "firstname": "Alice",
  "city": "San Francisco"
}
```

We added a new field "city" to each document, and updated the original document by adding a value for it.

```
Operation: Upsert#Bob
Document:
{
  "firstname": "Bob",
  "age": 25
}
```

We added a new document, no new fields were added. The following queries can be performed on IKV now:

```
"Alice#firstname" -> "Alice"
"Alice#age" -> 22
"Alice#city" -> "San Francisco"

"Bob#firstname" -> "Bob"
"Bob#age" -> 25
"Bob#city" -> // empty
```
