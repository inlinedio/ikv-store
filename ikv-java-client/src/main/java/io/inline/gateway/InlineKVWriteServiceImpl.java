package io.inline.gateway;

import com.google.rpc.Code;
import com.inlineio.schemas.Common.*;
import com.inlineio.schemas.InlineKVWriteServiceGrpc;
import com.inlineio.schemas.Services.*;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.inline.gateway.streaming.IKVWritesPublisher;

import java.util.*;
import java.util.stream.Collectors;

public class InlineKVWriteServiceImpl extends InlineKVWriteServiceGrpc.InlineKVWriteServiceImplBase {
    private final IKVWritesPublisher _ikvWritesPublisher;
    private final UserStoreContextFactory _userStoreContextFactory;

    public InlineKVWriteServiceImpl(IKVWritesPublisher ikvWritesPublisher,
                                    UserStoreContextFactory userStoreContextFactory) {
        _ikvWritesPublisher = Objects.requireNonNull(ikvWritesPublisher);
        _userStoreContextFactory = Objects.requireNonNull(userStoreContextFactory);
    }

    @Override
    public void upsertFieldValues(UpsertFieldValuesRequest request, StreamObserver<SuccessStatus> responseObserver) {
        IKVDocumentOnWire document = request.getDocument();

        UserStoreContextInitializer initializer = request.getUserStoreContextInitializer();
        Optional<UserStoreContext> maybeContext = _userStoreContextFactory.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }

        try {
            // write to kafka
            _ikvWritesPublisher.publishFieldUpserts(maybeContext.get(), Collections.singletonList(document.getDocumentMap()));
            _ikvWritesPublisher.publishFieldUpserts(maybeContext.get(), Collections.singletonList(document.getDocumentMap()));
        } catch (Exception e) {
            propagateError(e, responseObserver);
            return;
        }

        responseObserver.onNext(SuccessStatus.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void batchUpsertFieldValues(BatchUpsertFieldValuesRequest request, StreamObserver<SuccessStatus> responseObserver) {
        int batchSize = request.getDocumentsCount();
        if (batchSize == 0) {
            responseObserver.onNext(SuccessStatus.newBuilder().build());
            return;
        }

        UserStoreContextInitializer initializer = request.getUserStoreContextInitializer();
        Optional<UserStoreContext> maybeContext = _userStoreContextFactory.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }

        // This need not be a transaction, ok for a failure to happen for certain documents
        // The client can republish the entire batch if write for any single document fails
        // We return an error as soon as a single write fails
        Collection<Map<String, FieldValue>> fieldMaps = request.getDocumentsList()
                .stream().map(IKVDocumentOnWire::getDocumentMap).collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
        try {
            _ikvWritesPublisher.publishFieldUpserts(maybeContext.get(), fieldMaps);
        } catch (Exception e) {
            propagateError(e, responseObserver);
            return;
        }

        responseObserver.onNext(SuccessStatus.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteFieldValues(DeleteFieldValueRequest request, StreamObserver<SuccessStatus> responseObserver) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void batchDeleteFieldValues(BatchDeleteFieldValuesRequest request, StreamObserver<SuccessStatus> responseObserver) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void deleteDocument(DeleteDocumentRequest request, StreamObserver<SuccessStatus> responseObserver) {
        IKVDocumentOnWire documentId = request.getDocumentId();

        UserStoreContextInitializer initializer = request.getUserStoreContextInitializer();
        Optional<UserStoreContext> maybeContext = _userStoreContextFactory.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }

        try {
            // write to kafka
            _ikvWritesPublisher.publishDocumentDeletes(maybeContext.get(), Collections.singletonList(documentId.getDocumentMap()));
        } catch (Exception e) {
            propagateError(e, responseObserver);
            return;
        }

        responseObserver.onNext(SuccessStatus.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void batchDeleteDocuments(BatchDeleteDocumentsRequest request, StreamObserver<SuccessStatus> responseObserver) {
        int batchSize = request.getDocumentIdsCount();
        if (batchSize == 0) {
            responseObserver.onNext(SuccessStatus.newBuilder().build());
            return;
        }

        UserStoreContextInitializer initializer = request.getUserStoreContextInitializer();
        Optional<UserStoreContext> maybeContext = _userStoreContextFactory.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }

        // This need not be a transaction, ok for a failure to happen for certain documents
        // The client can republish the entire batch if write for any single document fails
        // We return an error as soon as a single write fails
        Collection<Map<String, FieldValue>> fieldMaps = request.getDocumentIdsList()
                .stream().map(IKVDocumentOnWire::getDocumentMap).collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
        try {
            _ikvWritesPublisher.publishDocumentDeletes(maybeContext.get(), fieldMaps);
        } catch (Exception e) {
            propagateError(e, responseObserver);
            return;
        }

        responseObserver.onNext(SuccessStatus.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void userStoreSchemaUpdate(UserStoreSchemaUpdateRequest request, StreamObserver<SuccessStatus> responseObserver) {
        UserStoreContextInitializer initializer = request.getUserStoreContextInitializer();
        Optional<UserStoreContext> maybeContext = _userStoreContextFactory.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }

        Collection<FieldSchema> newFieldsToAdd = request.getNewFieldsToAddList();
        try {
            maybeContext.get().updateSchema(newFieldsToAdd);
        } catch (Exception e) {
            propagateError(e, responseObserver);
            return;
        }

        responseObserver.onNext(SuccessStatus.newBuilder().build());
        responseObserver.onCompleted();
    }

    // TODO: better error handling
    private void propagateError(Exception e, StreamObserver<SuccessStatus> responseObserver) {
        if (e instanceof IllegalArgumentException | e instanceof NullPointerException) {
            com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                    .setCode(Code.INVALID_ARGUMENT.getNumber())
                    .setMessage("Invalid arguments")
                    .build();
            responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            return;
        }

        if (e instanceof InterruptedException || e instanceof RuntimeException) {
            com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage("Internal Error")
                    .build();
            responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            return;
        }

        // Catch all
        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(Code.UNKNOWN.getNumber())
                .setMessage("Unknown Internal Error")
                .build();
        responseObserver.onError(StatusProto.toStatusRuntimeException(status));
    }
}
