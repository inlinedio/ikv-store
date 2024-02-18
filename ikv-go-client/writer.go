package ikvclient

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"time"

	schemas "github.com/pushkarmoi/ikv-store/ikv-go-client/schemas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// see https://github.com/grpc/grpc-go/blob/master/examples/features/retry/client/main.go
	// see https://github.com/grpc/grpc/blob/master/doc/service_config.md to know more about service config
	retryPolicy = `{
		"methodConfig": [{
		  "name": [{"service": "ikvschemas.InlineKVWriteService"}],
		  "waitForReady": true,
		  "retryPolicy": {
			  "MaxAttempts": 3,
			  "InitialBackoff": ".01s",
			  "MaxBackoff": ".01s",
			  "BackoffMultiplier": 1.0,
			  "RetryableStatusCodes": [ "UNAVAILABLE" ]
		  }
		}]}`
)

type DefaultIKVWriter struct {
	connection              *grpc.ClientConn
	client                  schemas.InlineKVWriteServiceClient
	userStoreCtxInitializer *schemas.UserStoreContextInitializer
}

// Constructor for creating new instances.
func NewDefaultIKVWriter(clientOptions *ClientOptions) (*DefaultIKVWriter, error) {
	// create ctx initializer
	storename, exists := clientOptions.config.StringConfigs["store_name"]
	if !exists {
		return nil, errors.New("store_name is a required client specified option")
	}

	accountid, exists := clientOptions.config.StringConfigs["account_id"]
	if !exists {
		return nil, errors.New("account_id is a required client specified option")
	}

	accountpasskey, exists := clientOptions.config.StringConfigs["account_passkey"]
	if !exists {
		return nil, errors.New("account_passkey is a required client specified option")
	}

	userStoreCtxInitializer := schemas.UserStoreContextInitializer{
		StoreName: storename,
		Credentials: &schemas.AccountCredentials{
			AccountId:      accountid,
			AccountPasskey: accountpasskey,
		},
	}

	return &DefaultIKVWriter{userStoreCtxInitializer: &userStoreCtxInitializer}, nil
}

// Startup. Connection initialization.
func (writer *DefaultIKVWriter) Startup() error {
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: false})
	connection, err := grpc.Dial("gateway.inlined.io:443", grpc.WithTransportCredentials(creds), grpc.WithDefaultServiceConfig(retryPolicy))
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	writer.connection = connection
	writer.client = schemas.NewInlineKVWriteServiceClient(connection)

	return nil
}

// Shutdown. Teardown connection.
func (writer *DefaultIKVWriter) Shutdown() error {
	writer.connection.Close()
	return nil
}

// Upsert. Publish a document.
func (writer *DefaultIKVWriter) UpsertFields(document *IKVDocument) error {
	if len(document.fields) < 1 {
		return errors.New("empty document cannot be upserted")
	}

	// create request
	documentOnWire := schemas.IKVDocumentOnWire{Document: document.fields}
	request := schemas.UpsertFieldValuesRequest{
		UserStoreContextInitializer: writer.userStoreCtxInitializer,
		Timestamp:                   timestamppb.Now(),
		Document:                    &documentOnWire,
	}

	// make request
	// retries are made automatically for select errors (see policy above)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := writer.client.UpsertFieldValues(ctx, &request)

	return err
}

// Helper to fetch server supplied configs.
// TODO: move this out of the writer struct for
// better separation.
func (writer *DefaultIKVWriter) serverSuppliedConfig() (*schemas.IKVStoreConfig, error) {

	// create request
	request := schemas.GetUserStoreConfigRequest{
		UserStoreContextInitializer: writer.userStoreCtxInitializer,
	}

	// make request
	// retries are made automatically for select errors (see policy above)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := writer.client.GetUserStoreConfig(ctx, &request)
	if err != nil {
		return nil, err
	}

	return response.GlobalConfig, nil
}

func (writer *DefaultIKVWriter) Helloworld(input string) (*schemas.HelloWorldResponse, error) {

	request := schemas.HelloWorldRequest{Echo: input}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return writer.client.HelloWorld(ctx, &request)
}
