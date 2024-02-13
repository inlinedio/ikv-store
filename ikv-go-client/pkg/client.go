package ikvclient

type IKVReader interface {
	startup() error
	shutdown() error
	getBytesValue(key interface{}, fieldname string) ([]byte, error)
	getStringValue(key interface{}, fieldname string) (string, error)
}

type IKVWriter interface {
	startup() error
	shutdown() error
	upsertFields(document IKVDocument) error
}

type IKVClientFactory struct {
}

func (f *IKVClientFactory) createNewReader(clientOptions *ClientOptions) (IKVReader, error) {
	return &DefaultIKVReader{
		clientoptions: clientOptions,
	}, nil
}
