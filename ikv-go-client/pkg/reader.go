package ikvclient

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"inlined.io/ikv-go-client/schemas"
)

const bad_handle = -1

type DefaultIKVReader struct {
	clientoptions *ClientOptions
	nativeReader  *NativeReader
	handle        int64
}

// Startup. Reader fetches and combines server/client configs
// and opens embedded index via cgo.
func (reader *DefaultIKVReader) startup() error {
	if reader.handle != bad_handle {
		return nil
	}

	// fetch server supplied options, and override them with client options
	config, err := reader.createIKVConfig()
	if err != nil {
		return err
	}
	config_bytes, err := proto.Marshal(config)
	if err != nil {
		return err
	}

	// open embedded index reader
	handle, err := reader.nativeReader.open(config_bytes)
	if err != nil {
		return fmt.Errorf("Cannot initialize reader: %w", err)
	}
	reader.handle = handle

	return nil
}

// Shutdown. Reader invokes shutdown sequence on the embedded index
// via cgo.
func (reader *DefaultIKVReader) shutdown() error {
	if reader.handle == bad_handle {
		return nil
	}

	if err := reader.nativeReader.close(reader.handle); err != nil {
		return err
	}

	reader.handle = bad_handle
	return nil
}

func (reader *DefaultIKVReader) getBytesValue(key interface{}, fieldname string) ([]byte, error) {
	// TODO: handle more types
	// key can only be of type string or bytes for now.
	switch primaryKey := key.(type) {
	case string:
		return reader.nativeReader.getFieldValue(
			reader.handle,
			[]byte(primaryKey),
			fieldname), nil
	case []byte:
		return reader.nativeReader.getFieldValue(
			reader.handle,
			primaryKey,
			fieldname), nil
	default:
		return nil, errors.New("Key can only be a string or []byte")
	}
}

func (reader *DefaultIKVReader) getStringValue(key interface{}, fieldname string) (string, error) {
	return "", nil
}

func (reader *DefaultIKVReader) createIKVConfig() (*schemas.IKVStoreConfig, error) {
	client, err := NewDefaultIKVWriter(reader.clientoptions)
	if err != nil {
		return nil, fmt.Errorf("Cannot fetch server supplied options: %w", err)
	}

	config, err := client.serverSuppliedConfig()
	if err != nil {
		return nil, fmt.Errorf("Cannot fetch server supplied options: %w", err)
	}

	for k, v := range reader.clientoptions.config.StringConfigs {
		config.StringConfigs[k] = v
	}
	for k, v := range reader.clientoptions.config.IntConfigs {
		config.IntConfigs[k] = v
	}
	for k, v := range reader.clientoptions.config.FloatConfigs {
		config.FloatConfigs[k] = v
	}
	for k, v := range reader.clientoptions.config.BytesConfigs {
		config.BytesConfigs[k] = v
	}
	for k, v := range reader.clientoptions.config.BooleanConfigs {
		config.BooleanConfigs[k] = v
	}

	return config, nil
}
