package ikvclient

import (
	"errors"

	schemas "github.com/pushkarmoi/ikv-store/ikv-go-client/schemas"
)

// Represents an indexable document for IKV (collection of fields associated with a primary key).
// See IKVDocumentBuilder to build documents.
type IKVDocument struct {
	fields map[string]*schemas.FieldValue
}

type IKVDocumentBuilder struct {
	fields map[string]*schemas.FieldValue
	err    error
}

// Constructor for a new document builder.
func NewIKVDocumentBuilder() *IKVDocumentBuilder {
	return &IKVDocumentBuilder{fields: make(map[string]*schemas.FieldValue)}
}

// Insert a string field.
func (builder *IKVDocumentBuilder) PutStringField(fieldname string, value string) *IKVDocumentBuilder {
	if fieldname == "" || value == "" {
		builder.err = errors.New("missing field name or field value")
		return nil
	}

	builder.fields[fieldname] = &schemas.FieldValue{FieldType: schemas.FieldType_STRING, Value: []byte(value)}
	return builder
}

// Insert a bytes field.
func (builder *IKVDocumentBuilder) PutBytesField(fieldname string, value []byte) *IKVDocumentBuilder {
	if fieldname == "" || len(value) == 0 {
		builder.err = errors.New("missing field name or field value")
		return nil
	}

	builder.fields[fieldname] = &schemas.FieldValue{FieldType: schemas.FieldType_BYTES, Value: value}
	return builder
}

// Build this document.
func (builder *IKVDocumentBuilder) Build() (IKVDocument, error) {
	return IKVDocument{fields: builder.fields}, builder.err
}
