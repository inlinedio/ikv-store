package ikvclient

// See IKVClientFactory to create a new instance.
type IKVReader interface {
	startup() error
	shutdown() error
	getBytesValue(key interface{}, fieldname string) ([]byte, error)
	getStringValue(key interface{}, fieldname string) (string, error)
}

// See IKVClientFactory to create a new instance.
type IKVWriter interface {
	startup() error
	shutdown() error
	upsertFields(document IKVDocument) error
}

type IKVClientFactory struct {
}

// Create new IKV reader instance.
func (f *IKVClientFactory) createNewReader(clientOptions *ClientOptions) (IKVReader, error) {
	return &DefaultIKVReader{
		clientoptions: clientOptions,
	}, nil
}

// Create new IKV writer instance.
func (f *IKVClientFactory) createNewWriter(clientOptions *ClientOptions) (IKVWriter, error) {
	return NewDefaultIKVWriter(clientOptions)
}
