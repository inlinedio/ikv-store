package io.inline.clients;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.InlineKVWriteServiceGrpc;
import com.inlineio.schemas.Services.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.protobuf.StatusProto;

import java.time.Instant;
import java.util.Collection;

/** RPC based writer instance. */
public class GRPCInlineKVWriter implements InlineKVWriter {
    private volatile InlineKVWriteServiceGrpc.InlineKVWriteServiceBlockingStub _stub;
    private final UserStoreContextInitializer _userStoreCtxInitializer;

    public GRPCInlineKVWriter() {
        // TODO: create using ClientOptions.
        _userStoreCtxInitializer = UserStoreContextInitializer.newBuilder()
                .setStoreName("testing-store")
                .setCredentials(Common.AccountCredentials.newBuilder()
                        .setAccountId("testing-account")
                        .setAccountPasskey("testing-passkey")
                        .build())
                .build();
    }

    @Override
    public void startup() {
        // TODO: stub creation- use dns
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        _stub = InlineKVWriteServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void shutdown() {
        _stub = null;
    }

    @Override
    public void upsertFieldValues(IKVDocument document) {
        Preconditions.checkState(_stub != null, "client cannot be used before finishing startup() or after shutdown()");
        Preconditions.checkArgument(document.asMap().size() >= 1, "empty document not allowed");

        MultiFieldDocument multiFieldDocument = MultiFieldDocument.newBuilder()
                .putAllDocument(document.asMap())
                .build();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

        UpsertFieldValuesRequest request = UpsertFieldValuesRequest.newBuilder()
                .setMultiFieldDocument(multiFieldDocument)
                .setTimestamp(timestamp)
                .setUserStoreContextInitializer(_userStoreCtxInitializer)
                .build();

        try {
            // make grpc call
            SuccessStatus ignored = _stub.asyncUpsertFieldValues(request);
        } catch (Throwable thrown) {
            // propagate errors
            com.google.rpc.Status errorStatus = StatusProto.fromThrowable(thrown);
            if (errorStatus != null) {
                throw new RuntimeException("upsertFieldValues failed with error: " + MoreObjects.firstNonNull(errorStatus.getMessage(), "unknown"));
            }
        }
    }

    @Override
    public void batchUpsertFieldValues(Collection<IKVDocument> documents) {
        throw new UnsupportedOperationException("batch ops implementation pending.");
    }

    @Override
    public void deleteFieldValues(IKVDocument documentId, Collection<String> fieldsToDelete) {
        Preconditions.checkState(_stub != null, "client cannot be used before finishing startup() or after shutdown()");
        Preconditions.checkArgument(documentId.asMap().size() >= 1, "need document-identifiers");
        if (fieldsToDelete.isEmpty()) {
            return;
        }

        MultiFieldDocument docId = MultiFieldDocument.newBuilder()
                .putAllDocument(documentId.asMap())
                .build();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

        DeleteFieldValueRequest request = DeleteFieldValueRequest.newBuilder()
                .setDocumentId(docId)
                .addAllFieldNames(fieldsToDelete)
                .setTimestamp(timestamp)
                .setUserStoreContextInitializer(_userStoreCtxInitializer)
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
    public void batchDeleteFieldValues(Collection<IKVDocument> documentIds, Collection<String> fieldsToDelete) {
        throw new UnsupportedOperationException("batch ops implementation pending.");
    }

    @Override
    public void deleteDocument(IKVDocument documentId) {
        Preconditions.checkState(_stub != null, "client cannot be used before finishing startup() or after shutdown()");
        Preconditions.checkArgument(documentId.asMap().size() >= 1, "need document-identifiers");

        MultiFieldDocument docId = MultiFieldDocument.newBuilder()
                .putAllDocument(documentId.asMap())
                .build();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

        DeleteDocumentRequest request = DeleteDocumentRequest.newBuilder()
                .setDocumentId(docId)
                .setTimestamp(timestamp)
                .setUserStoreContextInitializer(_userStoreCtxInitializer)
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
    public void batchDeleteDocuments(Collection<IKVDocument> documentIds) {
        throw new UnsupportedOperationException("batch ops implementation pending.");
    }
}
