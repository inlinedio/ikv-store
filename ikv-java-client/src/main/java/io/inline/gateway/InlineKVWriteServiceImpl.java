package io.inline.gateway;

import com.google.common.base.Preconditions;
import com.google.rpc.Code;
import com.inlineio.schemas.Common.*;
import com.inlineio.schemas.InlineKVWriteServiceGrpc;
import com.inlineio.schemas.Services.*;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.inline.gateway.streaming.IKVKafkaWriter;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InlineKVWriteServiceImpl
    extends InlineKVWriteServiceGrpc.InlineKVWriteServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(InlineKVWriteServiceImpl.class);

  private final IKVKafkaWriter _ikvKafkaWriter;
  private final UserStoreContextAccessor _userStoreContextAccessor;

  public InlineKVWriteServiceImpl(
      IKVKafkaWriter ikvKafkaWriter, UserStoreContextAccessor userStoreContextAccessor) {
    _ikvKafkaWriter = Objects.requireNonNull(ikvKafkaWriter);
    _userStoreContextAccessor = Objects.requireNonNull(userStoreContextAccessor);
  }

  @Override
  public void helloWorld(
      HelloWorldRequest request, StreamObserver<HelloWorldResponse> responseObserver) {
    String echo = request.getEcho();
    LOGGER.info("Received helloWorld echo: {}", echo);

    // echo back
    responseObserver.onNext(HelloWorldResponse.newBuilder().setEcho(echo).build());
    responseObserver.onCompleted();
  }

  @Override
  public void upsertFieldValues(
      UpsertFieldValuesRequest request, StreamObserver<Status> responseObserver) {
    IKVDocumentOnWire document = request.getDocument();
    Collection<Map<String, FieldValue>> documents =
        Collections.singletonList(document.getDocumentMap());

    try {
      upsertDocumentsImpl(request.getUserStoreContextInitializer(), documents);
    } catch (Exception e) {
      LOGGER.debug("Error for upsertFieldValues: ", e);
      propagateError(e, responseObserver);
      return;
    }

    responseObserver.onNext(Status.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void batchUpsertFieldValues(
      BatchUpsertFieldValuesRequest request, StreamObserver<Status> responseObserver) {
    int batchSize = request.getDocumentsCount();
    Collection<Map<String, FieldValue>> documents =
        request.getDocumentsList().stream()
            .map(IKVDocumentOnWire::getDocumentMap)
            .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));

    try {
      upsertDocumentsImpl(request.getUserStoreContextInitializer(), documents);
    } catch (Exception e) {
      LOGGER.debug("Error for batchUpsertFieldValues: ", e);
      propagateError(e, responseObserver);
      return;
    }

    responseObserver.onNext(Status.newBuilder().build());
    responseObserver.onCompleted();
  }

  /**
   * Real implementation to publish a batch of documents (multi part).
   *
   * @param ctxInitializer for retrieving IKV store's metadata
   * @param documents to publish
   * @throws NullPointerException missing/null required request parameters
   * @throws IllegalArgumentException invalid user-context initializer
   */
  private void upsertDocumentsImpl(
      UserStoreContextInitializer ctxInitializer, Collection<Map<String, FieldValue>> documents) {
    Objects.requireNonNull(ctxInitializer); // error
    Objects.requireNonNull(documents); // error
    if (documents.isEmpty()) { // no op
      return;
    }

    Optional<UserStoreContext> maybeCtx = _userStoreContextAccessor.getCtx(ctxInitializer);
    Preconditions.checkArgument(
        maybeCtx.isPresent(), "Invalid store configuration or credentials provided");

    UserStoreContext ctx = maybeCtx.get();

    // This need not be a transaction, ok for a failure to happen for certain documents
    // The client can republish the entire batch if write for any single document fails
    // We return an error as soon as a single write fails.
    _ikvKafkaWriter.publishDocumentUpserts(ctx, documents);
  }

  @Override
  public void deleteFieldValues(
      DeleteFieldValueRequest request, StreamObserver<Status> responseObserver) {
    IKVDocumentOnWire documentId = request.getDocumentId();
    Collection<Map<String, FieldValue>> documentIds =
        Collections.singletonList(documentId.getDocumentMap());
    Collection<String> fieldNames = request.getFieldNamesList();

    try {
      deleteDocumentFieldsImpl(request.getUserStoreContextInitializer(), documentIds, fieldNames);
    } catch (Exception e) {
      LOGGER.debug("Error for deleteFieldValues: ", e);
      propagateError(e, responseObserver);
      return;
    }

    responseObserver.onNext(Status.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void batchDeleteFieldValues(
      BatchDeleteFieldValuesRequest request, StreamObserver<Status> responseObserver) {
    int batchSize = request.getDocumentIdsCount();
    Collection<Map<String, FieldValue>> documentIds =
        request.getDocumentIdsList().stream()
            .map(IKVDocumentOnWire::getDocumentMap)
            .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));
    Collection<String> fieldNames = request.getFieldNamesList();

    try {
      deleteDocumentFieldsImpl(request.getUserStoreContextInitializer(), documentIds, fieldNames);
    } catch (Exception e) {
      LOGGER.debug("Error for batchDeleteFieldValues: ", e);
      propagateError(e, responseObserver);
      return;
    }

    responseObserver.onNext(Status.newBuilder().build());
    responseObserver.onCompleted();
  }

  /**
   * Real implementation to delete fields from a batch of documents (given their document-ids).
   *
   * @param ctxInitializer for retrieving IKV store's metadata
   * @param documentIds to delete from
   * @param fieldNames to delete from documentIds
   * @throws NullPointerException missing/null required request parameters
   * @throws IllegalArgumentException invalid user-context initializer
   */
  private void deleteDocumentFieldsImpl(
      UserStoreContextInitializer ctxInitializer,
      Collection<Map<String, FieldValue>> documentIds,
      Collection<String> fieldNames) {
    Objects.requireNonNull(ctxInitializer); // error
    Objects.requireNonNull(documentIds); // error
    Objects.requireNonNull(fieldNames); // error
    if (documentIds.isEmpty() || fieldNames.isEmpty()) { // no op
      return;
    }

    Optional<UserStoreContext> maybeCtx = _userStoreContextAccessor.getCtx(ctxInitializer);
    Preconditions.checkArgument(
        maybeCtx.isPresent(), "Invalid store configuration or credentials provided");

    UserStoreContext ctx = maybeCtx.get();

    // This need not be a transaction, ok for a failure to happen for certain documents
    // The client can republish the entire batch if write for any single document fails
    // We return an error as soon as a single write fails.
    _ikvKafkaWriter.publishDocumentFieldDeletes(ctx, documentIds, fieldNames);
  }

  @Override
  public void deleteDocument(
      DeleteDocumentRequest request, StreamObserver<Status> responseObserver) {
    IKVDocumentOnWire documentId = request.getDocumentId();
    Collection<Map<String, FieldValue>> documentIds =
        Collections.singletonList(documentId.getDocumentMap());

    try {
      deleteDocumentsImpl(request.getUserStoreContextInitializer(), documentIds);
    } catch (Exception e) {
      LOGGER.debug("Error for deleteDocument: ", e);
      propagateError(e, responseObserver);
      return;
    }

    responseObserver.onNext(Status.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void batchDeleteDocuments(
      BatchDeleteDocumentsRequest request, StreamObserver<Status> responseObserver) {
    int batchSize = request.getDocumentIdsCount();
    Collection<Map<String, FieldValue>> documentIds =
        request.getDocumentIdsList().stream()
            .map(IKVDocumentOnWire::getDocumentMap)
            .collect(Collectors.toCollection(() -> new ArrayList<>(batchSize)));

    try {
      deleteDocumentsImpl(request.getUserStoreContextInitializer(), documentIds);
    } catch (Exception e) {
      LOGGER.debug("Error for batchDeleteDocuments: ", e);
      propagateError(e, responseObserver);
      return;
    }

    responseObserver.onNext(Status.newBuilder().build());
    responseObserver.onCompleted();
  }

  /**
   * Real implementation to delete a batch of documents (given their document-ids).
   *
   * @param ctxInitializer for retrieving IKV store's metadata
   * @param documentIds to delete
   * @throws NullPointerException missing/null required request parameters
   * @throws IllegalArgumentException invalid user-context initializer
   */
  private void deleteDocumentsImpl(
      UserStoreContextInitializer ctxInitializer, Collection<Map<String, FieldValue>> documentIds) {
    Objects.requireNonNull(ctxInitializer); // error
    Objects.requireNonNull(documentIds); // error
    if (documentIds.isEmpty()) { // no op
      return;
    }

    Optional<UserStoreContext> maybeCtx = _userStoreContextAccessor.getCtx(ctxInitializer);
    Preconditions.checkArgument(
        maybeCtx.isPresent(), "Invalid store configuration or credentials provided");

    UserStoreContext ctx = maybeCtx.get();

    // This need not be a transaction, ok for a failure to happen for certain documents
    // The client can republish the entire batch if write for any single document fails
    // We return an error as soon as a single write fails.
    _ikvKafkaWriter.publishDocumentDeletes(ctx, documentIds);
  }

  @Override
  public void getUserStoreConfig(
      GetUserStoreConfigRequest request,
      StreamObserver<GetUserStoreConfigResponse> responseObserver) {

    IKVStoreConfig ikvStoreConfig;
    try {
      ikvStoreConfig = getUserStoreConfigImpl(request.getUserStoreContextInitializer());
    } catch (Exception e) {
      LOGGER.debug("Error for getUserStoreConfig: ", e);
      propagateError(e, responseObserver);
      return;
    }

    GetUserStoreConfigResponse response =
        GetUserStoreConfigResponse.newBuilder().setGlobalConfig(ikvStoreConfig).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private IKVStoreConfig getUserStoreConfigImpl(UserStoreContextInitializer ctxInitializer) {
    Objects.requireNonNull(ctxInitializer); // error
    Optional<UserStoreContext> maybeCtx = _userStoreContextAccessor.getCtx(ctxInitializer);
    Preconditions.checkArgument(
        maybeCtx.isPresent(), "Invalid store configuration or credentials provided");

    UserStoreContext ctx = maybeCtx.get();
    return ctx.createGatewaySpecifiedConfigs();
  }

  // TODO: better error handling
  private void propagateError(Exception e, StreamObserver<?> responseObserver) {
    if (e instanceof IllegalArgumentException | e instanceof NullPointerException) {
      com.google.rpc.Status status =
          com.google.rpc.Status.newBuilder()
              .setCode(Code.INVALID_ARGUMENT.getNumber())
              .setMessage("Invalid arguments: " + e)
              .build();
      responseObserver.onError(StatusProto.toStatusRuntimeException(status));
      return;
    }

    if (e instanceof InterruptedException || e instanceof RuntimeException) {
      com.google.rpc.Status status =
          com.google.rpc.Status.newBuilder()
              .setCode(Code.INTERNAL.getNumber())
              .setMessage("Internal Error: " + e)
              .build();
      responseObserver.onError(StatusProto.toStatusRuntimeException(status));
      return;
    }

    // Catch all
    com.google.rpc.Status status =
        com.google.rpc.Status.newBuilder()
            .setCode(Code.UNKNOWN.getNumber())
            .setMessage("Unknown Internal Error: " + e)
            .build();
    responseObserver.onError(StatusProto.toStatusRuntimeException(status));
  }
}
