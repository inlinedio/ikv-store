package io.inline.benchmarks;

import com.google.common.base.Preconditions;
import io.inline.clients.*;

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
        String paramString = "mode:batch,num_entries:1000,batch_size:10";  // default
        if (args.length > 0) {
            paramString = args[0];
        }
        BenchmarkParams benchmarkParams = new BenchmarkParams(paramString);

        IKVLatencyBenchmarkWorkflow workflow = new IKVLatencyBenchmarkWorkflow(benchmarkParams);
        workflow.connect();
        Histogram histogram = new Histogram("IKVBenchmarks", 100000);
        workflow.initializeWithWrites(histogram);

        if ("single".equals(benchmarkParams.getStringParameter("mode").get().toLowerCase())) {
            workflow.benchmarkSingleGet(histogram);
        } else if ("batch".equals(benchmarkParams.getStringParameter("mode").get().toLowerCase())) {
            workflow.benchmarkBatchGet(histogram);
        } else {
            throw new UnsupportedOperationException();
        }

        workflow.shutdown();

        System.out.println(histogram);
        System.exit(0);
    }


    private static final FieldAccessor PROFILE_FIELD_ACCESSOR = FieldAccessor.bytesFieldAccessor("profile");

    private final TestingInlineKVReader _testingClient;

    private final KeyValuesGenerator _keyValuesGenerator;
    private final ConcurrentHashMap<KeyValuesGenerator.BytesKey, byte[]> _sourceOfTruth;

    // parameters
    private final int _numEntries;
    private final int _batchSize;

    public IKVLatencyBenchmarkWorkflow(BenchmarkParams params) {
        ClientOptions clientOptions = new ClientOptions.Builder()
                .withMountDirectory("/tmp/Benchmarks")
                .withStoreName("Benchmarks")
                .withAccountId("testing-account-v1")
                .withAccountPassKey("testing-account-passkey")
                .withNumericOverride("kafka_partition", 0)  // TODO - remove
                .build();

        _testingClient = new TestingInlineKVReader(clientOptions);

        _numEntries = params.getIntegerParameter("num_entries").get();
        _batchSize = params.getIntegerParameter("batch_size").get();

        _keyValuesGenerator = new KeyValuesGenerator(_numEntries);
        _sourceOfTruth = new ConcurrentHashMap<>(_numEntries);
    }

    @Override
    public void connect() {
        _testingClient.startup(null);
        // no op
    }

    @Override
    public void initializeWithWrites(Histogram unused) {
        for (int i = 0; i < _numEntries; i++) {
            KeyValuesGenerator.BytesKey key = _keyValuesGenerator.getKey(i);
            byte[] keyBytes = key.getInnerBytes();
            byte[] valueBytes = _keyValuesGenerator.getValueBytes(50, i);

            // Write to Inline KV
            IKVDocument ikvDocument = new IKVDocument.Builder()
                    .putBytesField("key", keyBytes)
                    .putBytesField("profile", valueBytes)
                    .build();
            _testingClient.upsertFieldValues(ikvDocument);

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
            PrimaryKey primaryKey = PrimaryKey.from(key.getInnerBytes());
            byte[] valueBytes = _sourceOfTruth.get(key);

            // IKV lookup
            Instant start = Instant.now();
            byte[] returnedValueBytes = _testingClient.getBytesValue(primaryKey, PROFILE_FIELD_ACCESSOR);
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
            List<PrimaryKey> primaryKeys = bytesKeys.stream().map(PrimaryKey::from).toList();

            Instant start = Instant.now();
            List<byte[]> returnedValues = _testingClient.multiGetBytesValue(primaryKeys, PROFILE_FIELD_ACCESSOR);
            Instant end = Instant.now();

            if (histogram != null) {
                histogram.captureLatency(
                        Duration.between(start, end).toNanos() / 1000L);
            }

            // assert on returned values
            for (int j = 0; j < bytesKeys.size(); j++) {
                byte[] key = bytesKeys.get(j);
                byte[] expectedValue = _sourceOfTruth.get(new KeyValuesGenerator.BytesKey(key));
                byte[] returnedValue = returnedValues.get(j);

                boolean isSame = Arrays.equals(expectedValue, returnedValue);
                Preconditions.checkArgument(isSame);
            }

            i += _batchSize;
        }
    }

    @Override
    public void shutdown() {
        _testingClient.shutdown();
    }
}
