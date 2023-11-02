package io.inline.benchmarks;

import com.google.common.base.Preconditions;
import io.inline.IKVClient;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IKVLatencyBenchmarkWorkflow implements LatencyBenchmarkWorkflow {
    // Usage:
    // java -Xms10g -Xmx10g -cp ikv-java-client-redis-single.jar io.inline.benchmarks.IKVLatencyBenchmarkWorkflow "num_entries:10000,batch_size:100"
    public static void main(String[] args) {
        // arg parsing
        String paramString = "num_entries:100000,batch_size:100";  // default
        if (args.length > 0) {
            paramString = args[0];
        }
        BenchmarkParams benchmarkParams = new BenchmarkParams(paramString);

        IKVLatencyBenchmarkWorkflow workflow = new IKVLatencyBenchmarkWorkflow(benchmarkParams);
        workflow.connect();
        Histogram histogram = new Histogram("IKVBenchmarks", 100000);
        workflow.initializeWithWrites(histogram);
        workflow.benchmarkBatchGet(histogram);
        workflow.shutdown();

        System.out.println(histogram);
        System.exit(0);
    }

    private final IKVClient _ikvClient;

    private final KeyValuesGenerator _keyValuesGenerator;
    private final ConcurrentHashMap<KeyValuesGenerator.BytesKey, byte[]> _sourceOfTruth;

    // parameters
    private final int _numEntries;
    private final int _batchSize;

    public IKVLatencyBenchmarkWorkflow(BenchmarkParams params) {
        // IKVClient.create_new("/tmp/benchmark", "/home/ubuntu/inlineio/ikv/src/schema/sample.yaml");
        // IKVClient.create_new("/tmp/benchmark", "/Users/pushkar/projects/inlineio/ikv/src/schema/sample.yaml");
        _ikvClient = IKVClient.create_new("/tmp/benchmark", "/home/ubuntu/inlineio/ikv/src/schema/sample.yaml");

        _numEntries = params.getIntegerParameter("num_entries").get();
        _batchSize = params.getIntegerParameter("batch_size").get();

        _keyValuesGenerator = new KeyValuesGenerator(_numEntries);
        _sourceOfTruth = new ConcurrentHashMap<>(_numEntries);
    }

    @Override
    public void connect() {
        // no op
    }

    @Override
    public void initializeWithWrites(Histogram unused) {
        for (int i = 0; i < _numEntries; i++) {
            KeyValuesGenerator.BytesKey key = _keyValuesGenerator.getKey(i);
            byte[] keyBytes = key.getInnerBytes();
            byte[] valueBytes = _keyValuesGenerator.getValueBytes(350, i);

            // Write to Inline KV
            _ikvClient.upsertFieldValue(keyBytes, valueBytes, "profile");

            // Write to internal SOT for assertions
            _sourceOfTruth.put(key, valueBytes);
        }
    }

    @Override
    public void benchmarkSingleGet(Histogram histogram) {
        // warmup run
        benchmarkSingleGetImpl(null);

        // actual run
        benchmarkSingleGetImpl(histogram);
    }

    private void benchmarkSingleGetImpl(@Nullable Histogram histogram) {
        for (int i = 0; i < _numEntries; i++) {
            KeyValuesGenerator.BytesKey key = _keyValuesGenerator.getKey(i);
            byte[] keyBytes = key.getInnerBytes();
            byte[] valueBytes = _sourceOfTruth.get(key);

            // IKV lookup
            Instant start = Instant.now();
            byte[] returnedValueBytes = _ikvClient.getBytesFieldValue(keyBytes, "profile");
            Instant end = Instant.now();

            if (histogram != null) {
                histogram.captureLatency(Duration.between(start, end).toNanos() / 1000L);
            }

            // assert on returned value
            boolean isSame = Arrays.equals(valueBytes, returnedValueBytes);
            Preconditions.checkArgument(isSame);
        }
    }

    @Override
    public void benchmarkBatchGet(Histogram histogram) {
        // Warmup iteration
        benchmarkBatchGetImpl(null);

        // Main iteration
        benchmarkBatchGetImpl(histogram);
    }

    private void benchmarkBatchGetImpl(@Nullable Histogram histogram) {
        int i = 0;
        while (i < _numEntries) {
            int startEntry = i;
            int endEntry = Math.min(i + _batchSize, _numEntries);
            List<byte[]> bytesKeys = _keyValuesGenerator.getKeyBatch(startEntry, endEntry).stream()
                    .map(KeyValuesGenerator.BytesKey::getInnerBytes).toList();

            Instant start = Instant.now();
            List<byte[]> returnedValues = _ikvClient.getBatchBytesFieldValue(bytesKeys, "profile");
            Instant end = Instant.now();

            if (histogram != null) {
                histogram.captureLatency(
                        Duration.between(start, end).toNanos() / 1000L);
            }

            // assert on returned values
            for (int j = 0; j < bytesKeys.size(); j++) {
                byte[] key = bytesKeys.get(i);
                byte[] expectedValue = _sourceOfTruth.get(new KeyValuesGenerator.BytesKey(key));
                byte[] returnedValue = returnedValues.get(j);

                boolean isSame = Arrays.equals(expectedValue, returnedValue);
                Preconditions.checkArgument(isSame);
            }

            i += _batchSize;
        }
    }

    public List<byte[]> getValuesTemp(List<byte[]> keys) {
        return _ikvClient.getBatchBytesFieldValue(keys, "profile");
    }

    @Override
    public void shutdown() {
        _ikvClient.close();
    }
}
