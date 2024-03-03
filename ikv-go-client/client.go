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
	// for the document and the field-name. Returns zero-value i.e. empty bytes slice when the document
	// or field are missing.
	//
	// Field types are set during upsert operations on IKVWriter. Reading a field in a type different
	// than its written type can have unintended results.
	//
	// Please note IKV readers are eventually-consistent in terms of "reading your writes".
	// There can be a very small delay between writing document (with IKVWriter) and reading them.
	GetBytesValue(primaryKey interface{}, fieldname string) ([]byte, error)

	// Fetch an inner field of type string, by providing the primary-key
	// for the document and the field-name. Returns zero-value i.e. empty string when the document
	// or field are missing.
	//
	// Field types are set during upsert operations on IKVWriter. Reading a field in a type different
	// than its written type can have unintended results.
	//
	// Please note IKV readers are eventually-consistent in terms of "reading your writes".
	// There can be a very small delay between writing document (with IKVWriter) and reading them.
	GetStringValue(primaryKey interface{}, fieldname string) (string, error)

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
