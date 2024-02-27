package ikvclient

import (
	"errors"
	"fmt"

	schemas "github.com/inlinedio/ikv-store/ikv-go-client/schemas"
	"google.golang.org/protobuf/proto"
)

type DefaultIKVReader struct {
	clientoptions *ClientOptions
	native_reader *NativeReaderV2
}

func NewDefaultIKVReader(clientOptions *ClientOptions) (IKVReader, error) {
	if clientOptions == nil {
		return nil, errors.New("clientOptions are required")
	}

	// no assertion on required options
	// will be done by native call
	return &DefaultIKVReader{
		clientoptions: clientOptions,
		native_reader: nil,
	}, nil
}

// Startup. Reader fetches and combines server/client configs
// and opens embedded index via cgo.
func (reader *DefaultIKVReader) Startup() error {
	// dynamic load native IKV binaries
	if reader.native_reader == nil {
		mountdir, exists := reader.clientoptions.config.StringConfigs["mount_directory"]
		if !exists {
			return errors.New("mount_directory is a required client specified option")
		}

		bin_manager, err := NewBinaryManager(mountdir)
		if err != nil {
			return err
		}

		dll_path, err := bin_manager.GetPathToNativeBinary()
		if err != nil {
			return err
		}

		reader.native_reader, err = NewNativeReaderV2(dll_path)
		if err != nil {
			return err
		}
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
	err = reader.native_reader.Open(config_bytes)
	if err != nil {
		return fmt.Errorf("cannot initialize reader: %w", err)
	}

	return nil
}

// Shutdown. Reader invokes shutdown sequence on the embedded index
// via cgo.
func (reader *DefaultIKVReader) Shutdown() error {
	if err := reader.native_reader.Close(); err != nil {
		return err
	}

	return nil
}

func (reader *DefaultIKVReader) HealthCheck() (bool, error) {
	return reader.native_reader.HealthCheck("healthcheck")
}

func (reader *DefaultIKVReader) GetBytesValue(key interface{}, fieldname string) ([]byte, error) {
	switch primaryKey := key.(type) {
	case string:
		return reader.native_reader.GetFieldValue(
			[]byte(primaryKey),
			fieldname), nil
	case []byte:
		return reader.native_reader.GetFieldValue(
			primaryKey,
			fieldname), nil
	default:
		return nil, errors.New("key can only be a string or []byte")
	}
}

func (reader *DefaultIKVReader) GetStringValue(key interface{}, fieldname string) (string, error) {
	bv, err := reader.GetBytesValue(key, fieldname)
	if err != nil {
		return "", err
	}

	return string(bv), nil
}

func (reader *DefaultIKVReader) createIKVConfig() (*schemas.IKVStoreConfig, error) {
	client, err := NewDefaultIKVWriter(reader.clientoptions)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch server supplied options: %w", err)
	}

	err = client.Startup()
	if err != nil {
		return nil, fmt.Errorf("cannot fetch server supplied options: %w", err)
	}

	config, err := client.serverSuppliedConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot fetch server supplied options: %w", err)
	}

	err = client.Shutdown()
	if err != nil {
		return nil, fmt.Errorf("cannot fetch server supplied options: %w", err)
	}

	if config.StringConfigs == nil {
		config.StringConfigs = make(map[string]string)
	}
	for k, v := range reader.clientoptions.config.StringConfigs {
		config.StringConfigs[k] = v
	}

	if config.IntConfigs == nil {
		config.IntConfigs = make(map[string]int64)
	}
	for k, v := range reader.clientoptions.config.IntConfigs {
		config.IntConfigs[k] = v
	}

	if config.FloatConfigs == nil {
		config.FloatConfigs = make(map[string]float32)
	}
	for k, v := range reader.clientoptions.config.FloatConfigs {
		config.FloatConfigs[k] = v
	}

	if config.BytesConfigs == nil {
		config.BytesConfigs = make(map[string][]byte)
	}
	for k, v := range reader.clientoptions.config.BytesConfigs {
		config.BytesConfigs[k] = v
	}

	if config.BooleanConfigs == nil {
		config.BooleanConfigs = make(map[string]bool)
	}
	for k, v := range reader.clientoptions.config.BooleanConfigs {
		config.BooleanConfigs[k] = v
	}

	return config, nil
}
