package ikvclient

import (
	"errors"
	"fmt"
	"strings"

	schemas "github.com/inlinedio/ikv-store/ikv-go-client/schemas"
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
	err    error
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
	builder.WithConsoleLogging("info")

	// TODO: remove explicit parititon
	builder.config.IntConfigs["partition"] = 0

	return &builder
}

func (builder *ClientOptionsBuilder) WithMountDirectory(dir string) *ClientOptionsBuilder {
	if dir == "" {
		builder.err = errors.New("mount directory cannot be empty")
		return nil
	}

	builder.config.StringConfigs["mount_directory"] = dir
	return builder
}

func (builder *ClientOptionsBuilder) WithStoreName(name string) *ClientOptionsBuilder {
	if name == "" {
		builder.err = errors.New("store name cannot be empty")
		return nil
	}

	builder.config.StringConfigs["store_name"] = name
	return builder
}

// TODO: allow store partition to be injected

func (builder *ClientOptionsBuilder) WithAccountId(accountId string) *ClientOptionsBuilder {
	if accountId == "" {
		builder.err = errors.New("account-id cannot be empty")
		return nil
	}

	builder.config.StringConfigs["account_id"] = accountId
	return builder
}

func (builder *ClientOptionsBuilder) WithAccountPasskey(accountPasskey string) *ClientOptionsBuilder {
	if accountPasskey == "" {
		builder.err = errors.New("account-passkey cannot be empty")
		return nil
	}

	builder.config.StringConfigs["account_passkey"] = accountPasskey
	return builder
}

// Ex. withKafkaPropertyOverride("ssl.ca.location", "/etc/ssl/certs")
// is required on Ubuntu hosts to declare CA certificates.
func (builder *ClientOptionsBuilder) WithKafkaPropertyOverride(key string, value string) *ClientOptionsBuilder {
	if key == "" || value == "" {
		builder.err = errors.New("kafka overrides cannot be empty")
		return nil
	}

	builder.config.StringConfigs["kafkaprop_"+key] = value
	return builder
}

func (builder *ClientOptionsBuilder) WithConsoleLogging(level string) *ClientOptionsBuilder {
	if level == "" {
		builder.err = errors.New("log level cannot be empty")
		return nil
	}

	level = strings.ToLower(level)
	if _, exists := loglevels[level]; !exists {
		builder.err = fmt.Errorf("Invalid log level- %s", level)
		return nil
	}

	builder.config.StringConfigs["rust_client_log_level"] = level
	builder.config.BooleanConfigs["rust_client_log_to_console"] = true
	return builder
}

func (builder *ClientOptionsBuilder) WithFileLogging(filepath string, level string) *ClientOptionsBuilder {
	if filepath == "" || level == "" {
		builder.err = errors.New("filepath or log level cannot be empty")
		return nil
	}

	level = strings.ToLower(level)
	if _, exists := loglevels[level]; !exists {
		builder.err = fmt.Errorf("Invalid log level- %s", level)
		return nil
	}

	builder.config.StringConfigs["rust_client_log_level"] = level
	builder.config.BooleanConfigs["rust_client_log_to_console"] = false
	builder.config.StringConfigs["rust_client_log_file"] = filepath
	return builder
}

func (builder *ClientOptionsBuilder) Build() (ClientOptions, error) {
	return ClientOptions{config: schemas.IKVStoreConfig{
		StringConfigs:  builder.config.StringConfigs,
		IntConfigs:     builder.config.IntConfigs,
		FloatConfigs:   builder.config.FloatConfigs,
		BytesConfigs:   builder.config.BytesConfigs,
		BooleanConfigs: builder.config.BooleanConfigs,
	}}, builder.err
}
