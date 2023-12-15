package io.inline.benchmarks;

public interface LatencyBenchmarkWorkflow {
  void connect();

  void initializeWithWrites(Histogram histogram);

  void benchmarkSingleGet(Histogram histogram);

  void benchmarkBatchGet(Histogram histogram);

  void shutdown();
}
