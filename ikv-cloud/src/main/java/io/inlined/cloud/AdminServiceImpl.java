package io.inlined.cloud;

import com.google.protobuf.Empty;
import com.inlineio.schemas.AdminServiceGrpc;
import com.inlineio.schemas.Services;
import io.grpc.stub.StreamObserver;

public class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {
  private static final int HEALTHY = 0;
  private static final int UNHEALTHY = 1;

  private volatile int _code;

  public AdminServiceImpl() {
    _code = UNHEALTHY;
  }

  public void markHealthy() {
    _code = HEALTHY;
  }

  public void markUnhealthy() {
    _code = UNHEALTHY;
  }

  @Override
  public void healthCheck(
      Empty request, StreamObserver<Services.HealthCheckResponse> responseObserver) {
    responseObserver.onNext(Services.HealthCheckResponse.newBuilder().setCode(_code).build());
    responseObserver.onCompleted();
  }
}
