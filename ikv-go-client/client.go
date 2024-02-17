package ikvclient

// See IKVClientFactory to create a new instance.
type IKVReader interface {
	Startup() error
	Shutdown() error
	GetBytesValue(key interface{}, fieldname string) ([]byte, error)
	GetStringValue(key interface{}, fieldname string) (string, error)
}

// See IKVClientFactory to create a new instance.
type IKVWriter interface {
	Startup() error
	Shutdown() error
	UpsertFields(document *IKVDocument) error
}

type IKVClientFactory struct {
}

// Create new IKV reader instance.
func (f *IKVClientFactory) CreateNewReader(clientOptions *ClientOptions) (IKVReader, error) {
	return &DefaultIKVReader{
		clientoptions: clientOptions,
		nativeReader:  &NativeReader{},
		handle:        bad_handle,
	}, nil
}

// Create new IKV writer instance.
func (f *IKVClientFactory) CreateNewWriter(clientOptions *ClientOptions) (IKVWriter, error) {
	return NewDefaultIKVWriter(clientOptions)
}
