package io.inline.gateway;

import com.google.rpc.Code;
import com.inlineio.schemas.Common.*;
import com.inlineio.schemas.InlineKVWriteServiceGrpc;
import com.inlineio.schemas.Services.*;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.inline.gateway.streaming.KafkaProducerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class InlineKVWriteServiceImpl extends InlineKVWriteServiceGrpc.InlineKVWriteServiceImplBase {
    private static final Logger LOGGER = LogManager.getLogger(InlineKVWriteServiceImpl.class);

    private final IKVWriter _ikvWriter;
    private final UserStoreContextAccessor _userStoreContextAccessor;

    public InlineKVWriteServiceImpl(IKVWriter ikvWriter,
                                    UserStoreContextAccessor userStoreContextAccessor) {
        _ikvWriter = Objects.requireNonNull(ikvWriter);
        _userStoreContextAccessor = Objects.requireNonNull(userStoreContextAccessor);
    }

    @Override
    public void upsertFieldValues(UpsertFieldValuesRequest request, StreamObserver<SuccessStatus> responseObserver) {
        IKVDocumentOnWire document = request.getDocument();

        UserStoreContextInitializer initializer = request.getUserStoreContextInitializer();
        Optional<UserStoreContext> maybeContext = _userStoreContextAccessor.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }

        try {
            // write to kafka
            _ikvWriter.publishFieldUpserts(maybeContext.get(), Collections.singletonList(document.getDocumentMap()));
            _ikvWriter.publishFieldUpserts(maybeContext.get(), Collections.singletonList(document.getDocumentMap()));
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
        Optional<UserStoreContext> maybeContext = _userStoreContextAccessor.getCtx(initializer);
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
            _ikvWriter.publishFieldUpserts(maybeContext.get(), fieldMaps);
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
        Optional<UserStoreContext> maybeContext = _userStoreContextAccessor.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }

        try {
            // write to kafka
            _ikvWriter.publishDocumentDeletes(maybeContext.get(), Collections.singletonList(documentId.getDocumentMap()));
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
        Optional<UserStoreContext> maybeContext = _userStoreContextAccessor.getCtx(initializer);
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
            _ikvWriter.publishDocumentDeletes(maybeContext.get(), fieldMaps);
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
        Optional<UserStoreContext> maybeContext = _userStoreContextAccessor.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }

        // Update context
        try {
            _userStoreContextAccessor.registerSchemaForNewFields(initializer, request.getNewFieldsToAddList());
        } catch (Exception e) {
            propagateError(e, responseObserver);
            return;
        }

        // Broadcast to all readers
        maybeContext = _userStoreContextAccessor.getCtx(initializer);
        try {
            _ikvWriter.publishFieldSchemaUpdates(maybeContext.get(), request.getNewFieldsToAddList());
        } catch (Exception e) {
            propagateError(e, responseObserver);
            return;
        }

        responseObserver.onNext(SuccessStatus.newBuilder().build());
        responseObserver.onCompleted();
    }


    @Override
    public void getUserStoreConfig(GetUserStoreConfigRequest request, StreamObserver<GetUserStoreConfigResponse> responseObserver) {
        UserStoreContextInitializer initializer = request.getUserStoreContextInitializer();
        Optional<UserStoreContext> maybeContext = _userStoreContextAccessor.getCtx(initializer);
        if (maybeContext.isEmpty()) {
            Exception e = new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
            propagateError(e, responseObserver);
            return;
        }


        UserStoreContext context = maybeContext.get();
        IKVStoreConfig globalConfig = IKVStoreConfig.newBuilder()
                .putStringConfigs(IKVStoreConfigConstants.PRIMARY_KEY_FIELD_NAME, context.primaryKeyFieldName())
                .putStringConfigs(IKVStoreConfigConstants.PARTITIONING_KEY_FIELD_NAME, context.partitioningKeyFieldName())
                .putNumericConfigs(IKVStoreConfigConstants.NUM_KAFKA_PARTITIONS, context.numKafkaPartitions())
                .putStringConfigs(IKVStoreConfigConstants.KAFKA_CONSUMER_BOOTSTRAP_SERVER, KafkaProducerFactory.KAFKA_BOOTSTRAP_SERVER)
                .putStringConfigs(IKVStoreConfigConstants.KAFKA_CONSUMER_TOPIC_NAME, context.kafkaTopic())
                .build();

        GetUserStoreConfigResponse response = GetUserStoreConfigResponse.newBuilder()
                .setGlobalConfig(globalConfig)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // TODO: better error handling
    private void propagateError(Exception e, StreamObserver<?> responseObserver) {
        if (e instanceof IllegalArgumentException | e instanceof NullPointerException) {
            com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                    .setCode(Code.INVALID_ARGUMENT.getNumber())
                    .setMessage("Invalid arguments: " + e)
                    .build();
            responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            return;
        }

        if (e instanceof InterruptedException || e instanceof RuntimeException) {
            com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage("Internal Error: " + e)
                    .build();
            responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            return;
        }

        // Catch all
        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(Code.UNKNOWN.getNumber())
                .setMessage("Unknown Internal Error: " + e)
                .build();
        responseObserver.onError(StatusProto.toStatusRuntimeException(status));
    }
}
