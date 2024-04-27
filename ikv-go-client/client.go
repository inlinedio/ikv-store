package ikvclient

// See https://docs.inlined.io for detailed Go usage guide.

// Reader client over a provisioned IKV store.
// You should reuse an instantiated client as it holds an embedded database instance.
// Lifecycle (startup/shutdown) of a reader client should be tied with application lifecycle.
//
// See IKVClientFactory to create a new instance.
type IKVReader interface {
	// Blocking startup, initializes the embedded store by pulling latest data locally.
	// Do not invoke other functions before startup is finished.
	Startup() error

	// Shutdown embedded store - ex. during application exit.
	Shutdown() error

	// Fetch an inner field of type []byte, by providing the primary-key
	// for the document and the field-name.
	//
	// Return values: bool (if the field exists for the document), the value, optional error
	//
	// Field types are set during upsert operations on IKVWriter. Reading a field in a type different
	// than its written type can have unintended results.
	//
	// Please note IKV readers are eventually-consistent in terms of "reading your writes".
	// There can be a very small delay between writing document (with IKVWriter) and reading them.
	GetBytesValue(primaryKey interface{}, fieldname string) (bool, []byte, error)

	// Multi-get version of GetBytesValue (multiple primary-keys and multiple fields).
	//
	// Args:
	// primaryKeys - documents to fetch fields for, of type string or []byte, nil not allowed
	// fieldNames - fields to fetch as a slice of string
	//
	// Returns field values as slice of byte slices, in document order, i.e:
	// [doc0-field0][doc0-field1]..[doc0-fieldN][doc1-field0][doc1-field1]...[docN-fieldN]
	// The inner byte slices will be nil ([]byte(nil)) if the field does not exist for the document.
	MultiGetBytesValues(primaryKeys []interface{}, fieldNames []string) ([][]byte, error)

	// Fetch an inner field of type string, by providing the primary-key
	// for the document and the field-name.
	//
	// Return values: bool (if the field exists for the document), the value, optional error
	//
	// Field types are set during upsert operations on IKVWriter. Reading a field in a type different
	// than its written type can have unintended results.
	//
	// Please note IKV readers are eventually-consistent in terms of "reading your writes".
	// There can be a very small delay between writing document (with IKVWriter) and reading them.
	GetStringValue(primaryKey interface{}, fieldname string) (bool, string, error)

	HealthCheck() (bool, error)
}

// Writer client over a provisioned IKV store.
// You should reuse an instantiated client.
// Lifecycle (startup/shutdown) of a reader client should be tied with application lifecycle.
//
// See IKVClientFactory to create a new instance.
type IKVWriter interface {
	// Blocking startup, initializes the writer client.
	// Do not invoke other functions before startup is finished.
	Startup() error

	// Shutdown writer client - ex. during application exit.
	Shutdown() error

	// Upsert (insert or update) fields for a document.
	// Provided `document` must contain the value of the primary-key and (if-applicable) partitioning-key.
	//
	// Different values for the same primary-key are aggregated by unionizing distincts and overwriting duplicates. Example -
	// upsert: {"pkey": 0, "key1": "value1"}, followed by upsert: {"pkey": 0, "key1": "value100", "key2": "value2"}
	// results in {"pkey": 0, "key1": "value100", "key2": "value2"} being saved.
	UpsertFields(document *IKVDocument) error

	// Delete specified fields from a document. NO OP if document or any field in `fieldsToDelete` does not exist.
	// Provided `document` must contain the value of the primary-key and (if-applicable) partitioning-key.
	DeleteFields(document *IKVDocument, fieldsToDelete []string) error

	// Delete a document (i.e. all of its fields) if it exists. NO OP if `document` is not indexed.
	// Provided `document` must contain the value of the primary-key and (if-applicable) partitioning-key.
	DeleteDocument(document *IKVDocument) error

	// Drop specified fields for all documents.
	// Attempts to drop primary-key field are silently ignored (no error).
	DropFieldsByName(fieldNames []string) error

	// Drop specified fields for all documents, by specifying field name prefixes.
	// Attempts to drop primary-key field are silently ignored (no error).
	DropFieldsByNamePrefix(fieldNamePrefixes []string) error

	// Drop all documents from the store.
	DropAllFields() error

	HealthCheck() (bool, error)
}

// Factory for creating reader/writer clients.
// See `ClientOptions` struct to specify client configuration.
type IKVClientFactory struct {
}

// Create new IKV reader instance.
func (f *IKVClientFactory) CreateNewReader(clientOptions *ClientOptions) (IKVReader, error) {
	return NewDefaultIKVReader(clientOptions)
}

// Create new IKV writer instance.
func (f *IKVClientFactory) CreateNewWriter(clientOptions *ClientOptions) (IKVWriter, error) {
	return NewDefaultIKVWriter(clientOptions)
}
