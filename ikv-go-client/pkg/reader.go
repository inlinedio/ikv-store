package ikvclient

type DefaultIKVReader struct {
	clientoptions *ClientOptions
}

func (reader *DefaultIKVReader) startup() error {
	return nil
}

func (reader *DefaultIKVReader) shutdown() error {
	return nil
}

func (reader *DefaultIKVReader) getBytesValue(key interface{}, fieldname string) ([]byte, error) {
	return nil, nil
}

func (reader *DefaultIKVReader) getStringValue(key interface{}, fieldname string) (string, error) {
	return "", nil
}
