package io.inline;

import io.inline.benchmarks.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.List;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
public class DBBenchmarks {
    private IKVLatencyBenchmarkWorkflow _ikvworkflow;
    private RedisLatencyBenchmarkWorkflow _redisworkflow;
    private List<byte[]> _keys;
    private byte[][] _rediskeys;

    @Setup(Level.Invocation)
    public void setup() {
        KeyValuesGenerator keyValuesGenerator = new KeyValuesGenerator(1000000);
        List<String> stringKeys = keyValuesGenerator.getKeyBatch(0, 10).stream().toList();
        _keys = stringKeys.stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).toList();
        _rediskeys = _keys.toArray(new byte[0][]);

        BenchmarkParams benchmarkParams = new BenchmarkParams("num_entries:100000,batch_size:100");

        _ikvworkflow = new IKVLatencyBenchmarkWorkflow(benchmarkParams);
        _ikvworkflow.connect();
        Histogram histogram = new Histogram("noop", 100000);
        _ikvworkflow.initializeWithWrites(histogram);

        _redisworkflow = new RedisLatencyBenchmarkWorkflow(benchmarkParams);
        _redisworkflow.connect();
        _redisworkflow.initializeWithWrites(histogram);
    }
    @Benchmark
    public void ikvBenchmark(Blackhole bh) {
        List<byte[]> values = _ikvworkflow.getValuesTemp(_keys);
        bh.consume(values);
    }

    @Benchmark
    public void redisBenchmark(Blackhole bh) {
        List<byte[]> values = _redisworkflow.getValuesTemp(_rediskeys);
        bh.consume(values);
    }
}
