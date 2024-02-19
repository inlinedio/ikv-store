package ikvclient

// See IKVClientFactory to create a new instance.
type IKVReader interface {
	Startup() error
	Shutdown() error
	GetBytesValue(key interface{}, fieldname string) ([]byte, error)
	GetStringValue(key interface{}, fieldname string) (string, error)
	HealthCheck() (bool, error)
}

// See IKVClientFactory to create a new instance.
type IKVWriter interface {
	Startup() error
	Shutdown() error
	UpsertFields(document *IKVDocument) error
	HealthCheck() (bool, error)
}

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
