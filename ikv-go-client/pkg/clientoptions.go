package ikvclient

import (
	"errors"
	"fmt"
	"strings"

	schemas "inlined.io/ikv-go-client/schemas"
)

var loglevels = map[string]int{
	"error": 0,
	"warn":  0,
	"info":  0,
	"debug": 0,
	"trace": 0,
}

type ClientOptions struct {
	config schemas.IKVStoreConfig
}

type ClientOptionsBuilder struct {
	config schemas.IKVStoreConfig
}

// Create a new options builder.
func NewClientOptionsBuilder() *ClientOptionsBuilder {
	var builder = ClientOptionsBuilder{
		config: schemas.IKVStoreConfig{
			StringConfigs:  make(map[string]string),
			IntConfigs:     make(map[string]int64),
			FloatConfigs:   make(map[string]float32),
			BytesConfigs:   make(map[string][]byte),
			BooleanConfigs: make(map[string]bool),
		},
	}

	// set console logging with level = info by default
	builder.withConsoleLogging("info")

	return &builder
}

func (builder *ClientOptionsBuilder) withMountDirectory(dir string) (*ClientOptionsBuilder, error) {
	if dir == "" {
		return nil, errors.New("mount directory cannot be empty")
	}

	builder.config.StringConfigs["mount_directory"] = dir
	return builder, nil
}

func (builder *ClientOptionsBuilder) withStoreName(name string) (*ClientOptionsBuilder, error) {
	if name == "" {
		return nil, errors.New("store name cannot be empty")
	}

	builder.config.StringConfigs["store_name"] = name
	return builder, nil
}

// TODO: allow store partition to be injected

func (builder *ClientOptionsBuilder) withAccountId(accountId string) (*ClientOptionsBuilder, error) {
	if accountId == "" {
		return nil, errors.New("account-id cannot be empty")
	}

	builder.config.StringConfigs["account_id"] = accountId
	return builder, nil
}

func (builder *ClientOptionsBuilder) withAccountPasskey(accountPasskey string) (*ClientOptionsBuilder, error) {
	if accountPasskey == "" {
		return nil, errors.New("account-passkey cannot be empty")
	}

	builder.config.StringConfigs["account_passkey"] = accountPasskey
	return builder, nil
}

// Ex. withKafkaPropertyOverride("ssl.ca.location", "/etc/ssl/certs")
// is required on Ubuntu hosts to declare CA certificates.
func (builder *ClientOptionsBuilder) withKafkaPropertyOverride(key string, value string) (*ClientOptionsBuilder, error) {
	if key == "" || value == "" {
		return nil, errors.New("kafka overrides cannot be empty")
	}

	builder.config.StringConfigs["kafkaprop_"+key] = value
	return builder, nil
}

func (builder *ClientOptionsBuilder) withConsoleLogging(level string) (*ClientOptionsBuilder, error) {
	if level == "" {
		return nil, errors.New("log level cannot be empty")
	}

	level = strings.ToLower(level)
	if _, exists := loglevels[level]; !exists {
		return nil, fmt.Errorf("Invalid log level- %s", level)
	}

	builder.config.StringConfigs["rust_client_log_level"] = level
	builder.config.BooleanConfigs["rust_client_log_to_console"] = true
	return builder, nil
}

func (builder *ClientOptionsBuilder) withFileLogging(filepath string, level string) (*ClientOptionsBuilder, error) {
	if filepath == "" || level == "" {
		return nil, errors.New("filepath or log level cannot be empty")
	}

	level = strings.ToLower(level)
	if _, exists := loglevels[level]; !exists {
		return nil, fmt.Errorf("Invalid log level- %s", level)
	}

	builder.config.StringConfigs["rust_client_log_level"] = level
	builder.config.BooleanConfigs["rust_client_log_to_console"] = false
	builder.config.StringConfigs["rust_client_log_file"] = filepath
	return builder, nil
}

func (builder *ClientOptionsBuilder) build() ClientOptions {
	return ClientOptions{config: schemas.IKVStoreConfig{
		StringConfigs:  builder.config.StringConfigs,
		IntConfigs:     builder.config.IntConfigs,
		FloatConfigs:   builder.config.FloatConfigs,
		BytesConfigs:   builder.config.BytesConfigs,
		BooleanConfigs: builder.config.BooleanConfigs,
	}}
}
