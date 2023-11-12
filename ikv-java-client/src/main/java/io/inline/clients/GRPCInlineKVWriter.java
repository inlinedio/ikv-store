package io.inline.clients;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import com.inlineio.schemas.InlineKVWriteServiceGrpc;
import com.inlineio.schemas.Services.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.protobuf.StatusProto;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

/** RPC based writer instance. */
public class GRPCInlineKVWriter implements InlineKVWriter {
    private volatile InlineKVWriteServiceGrpc.InlineKVWriteServiceBlockingStub _stub;

    @Override
    public void startup() {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        _stub = InlineKVWriteServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void shutdown() {
        _stub = null;
    }

    @Override
    public void upsertFieldValues(Map<String, FieldValue> docFieldValues) {
        Preconditions.checkState(_stub != null, "client cannot be used before finishing startup() or after shutdown()");
        Preconditions.checkArgument(docFieldValues.size() > 1, "empty docFieldValues");

        MultiFieldDocument multiFieldDocument = MultiFieldDocument.newBuilder()
                .putAllDocument(docFieldValues)
                .build();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

        UpsertFieldValuesRequest request = UpsertFieldValuesRequest.newBuilder()
                .setMultiFieldDocument(multiFieldDocument)
                .setTimestamp(timestamp)
                .build();

        try {
            // make grpc call
            SuccessStatus _ignored = _stub.asyncUpsertFieldValues(request);
        } catch (Throwable thrown) {
            // propagate errors
            com.google.rpc.Status errorStatus = StatusProto.fromThrowable(thrown);
            if (errorStatus != null) {
                throw new RuntimeException("upsertFieldValues failed with error: " + MoreObjects.firstNonNull(errorStatus.getMessage(), "unknown"));
            }
        }
    }

    @Override
    public void batchUpsertFieldValues(Collection<Map<String, FieldValue>> docFieldValuesCollection) {
        throw new UnsupportedOperationException("batch ops implementation pending.");
    }

    @Override
    public void deleteFieldValues(Map<String, FieldValue> documentId, Collection<String> fieldsToDelete) {
        Preconditions.checkState(_stub != null, "client cannot be used before finishing startup() or after shutdown()");
        Preconditions.checkArgument(documentId.size() > 1, "need document-identifiers");
        Preconditions.checkArgument(fieldsToDelete.size() > 0, "fieldsToDelete can't be empty");

        MultiFieldDocument docId = MultiFieldDocument.newBuilder()
                .putAllDocument(documentId)
                .build();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

        DeleteFieldValueRequest request = DeleteFieldValueRequest.newBuilder()
                .setDocumentId(docId)
                .addAllFieldNames(fieldsToDelete)
                .setTimestamp(timestamp)
                .build();

        try {
            // make grpc call
            SuccessStatus _ignored = _stub.asyncDeleteFieldValues(request);
        } catch (Throwable thrown) {
            // propagate errors
            com.google.rpc.Status errorStatus = StatusProto.fromThrowable(thrown);
            if (errorStatus != null) {
                throw new RuntimeException("deleteFieldValues failed with error: " + MoreObjects.firstNonNull(errorStatus.getMessage(), "unknown"));
            }
        }
    }

    @Override
    public void batchDeleteFieldValues(Collection<Map<String, FieldValue>> documentIds, Collection<String> fieldsToDelete) {
        throw new UnsupportedOperationException("batch ops implementation pending.");
    }

    @Override
    public void deleteDocument(Map<String, FieldValue> documentId) {
        Preconditions.checkState(_stub != null, "client cannot be used before finishing startup() or after shutdown()");
        Preconditions.checkArgument(documentId.size() > 1, "need document-identifiers");

        MultiFieldDocument docId = MultiFieldDocument.newBuilder()
                .putAllDocument(documentId)
                .build();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

        DeleteDocumentRequest request = DeleteDocumentRequest.newBuilder()
                .setDocumentId(docId)
                .setTimestamp(timestamp)
                .build();

        try {
            // make grpc call
            SuccessStatus _ignored = _stub.asyncDeleteDocument(request);
        } catch (Throwable thrown) {
            // propagate errors
            com.google.rpc.Status errorStatus = StatusProto.fromThrowable(thrown);
            if (errorStatus != null) {
                throw new RuntimeException("deleteDocument failed with error: " + MoreObjects.firstNonNull(errorStatus.getMessage(), "unknown"));
            }
        }
    }

    @Override
    public void batchDeleteDocuments(Collection<Map<String, FieldValue>> documentIds) {
        throw new UnsupportedOperationException("batch ops implementation pending.");
    }
}
