package ikvclient

import (
	"errors"

	schemas "inlined.io/ikv-go-client/schemas"
)

type IKVDocument struct {
	fields map[string]schemas.FieldValue
}

type IKVDocumentBuilder struct {
	fields map[string]schemas.FieldValue
}

// Insert a string field.
func (builder *IKVDocumentBuilder) putStringField(fieldname string, value string) (*IKVDocumentBuilder, error) {
	if fieldname == "" || value == "" {
		return nil, errors.New("Missing field name or field value")
	}

	builder.fields[fieldname] = schemas.FieldValue{FieldType: schemas.FieldType_STRING, Value: []byte(value)}
	return builder, nil
}

// Insert a bytes field.
func (builder *IKVDocumentBuilder) putBytesField(fieldname string, value []byte) (*IKVDocumentBuilder, error) {
	if fieldname == "" || len(value) == 0 {
		return nil, errors.New("Missing field name or field value")
	}

	builder.fields[fieldname] = schemas.FieldValue{FieldType: schemas.FieldType_BYTES, Value: value}
	return builder, nil
}

// Build this document.
func (builder *IKVDocumentBuilder) build() (IKVDocument, error) {
	return IKVDocument{fields: builder.fields}, nil
}
