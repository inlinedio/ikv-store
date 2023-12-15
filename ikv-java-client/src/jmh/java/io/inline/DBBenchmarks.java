package io.inline;

import io.inline.benchmarks.*;
import java.util.List;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
public class DBBenchmarks {
  private IKVLatencyBenchmarkWorkflow _ikvworkflow;
  private RedisLatencyBenchmarkWorkflow _redisworkflow;
  private List<byte[]> _keys;
  private byte[][] _rediskeys;

  @Setup(Level.Invocation)
  public void setup() {}

  @Benchmark
  public void ikvBenchmark(Blackhole bh) {}

  @Benchmark
  public void redisBenchmark(Blackhole bh) {}
}
