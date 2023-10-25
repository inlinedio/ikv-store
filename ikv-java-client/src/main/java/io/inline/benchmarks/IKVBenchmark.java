package io.inline.benchmarks;

import com.google.common.base.Preconditions;
import io.inline.IKVClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IKVBenchmark {
    private final IKVClient _ikvClient;
    private final ConcurrentHashMap<String, byte[]> _hashmap;
    private final KeyValuesGenerator _keyValuesGenerator;
    private final int _numEntries;
    private final int _batchSize;


    public IKVBenchmark(IKVClient ikvClient, BenchmarkParams benchmarkParams) {
        _ikvClient = Preconditions.checkNotNull(ikvClient);
        _hashmap = new ConcurrentHashMap<>(10_000);
        _numEntries = benchmarkParams.getIntegerParameter("num_entries").get();
        _batchSize = benchmarkParams.getIntegerParameter("batch_size").get();
        _keyValuesGenerator = new KeyValuesGenerator(_numEntries);
    }

    // write data
    public void initialize() {
        for (int i = 0; i < _numEntries; i++) {
            String key = _keyValuesGenerator.getKey(i);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            _ikvClient.upsertFieldValue(
                    keyBytes,
                    KeyValuesGenerator.VALUE_BYTES,
                    "firstname");

            _hashmap.put(key, KeyValuesGenerator.VALUE_BYTES);
        }
    }

    public void benchmarkBatchGet(int warmupIterations) {
        for (int i = 0; i < warmupIterations; i++) {
            BenchmarkHistogram benchmarkHistogram = new BenchmarkHistogram("noop", _numEntries);
            benchmarkBatchGetImpl(benchmarkHistogram);
        }

        BenchmarkHistogram benchmarkHistogram = new BenchmarkHistogram("batchGetLookup", _numEntries);
        benchmarkBatchGetImpl(benchmarkHistogram);

        System.out.println(benchmarkHistogram);
    }

    private void benchmarkBatchGetImpl(BenchmarkHistogram benchmarkHistogram) {
        int i = 0;
        while (i < _numEntries) {
            int startEntry = i;
            int endEntry = Math.min(i + _batchSize, _numEntries);
            List<String> keys = _keyValuesGenerator.getKeyBatch(startEntry, endEntry)
                    .stream().toList();
            byte[] key = keys.get(0).getBytes(StandardCharsets.UTF_8);
            // String key = keys.get(0);

            Instant start = Instant.now();
            byte[] result = _ikvClient.getFieldValue(key, "firstname");
            // byte[] result = _hashmap.get(key);
            Instant end = Instant.now();

            benchmarkHistogram.setLatency(
                    Duration.between(start, end).toNanos() / 1000L);

            Preconditions.checkArgument(Arrays.equals(KeyValuesGenerator.VALUE_BYTES, result));

            /* for (Map.Entry<String, String> entry: results.entrySet()) {
                String fetchedValue = entry.getValue();
                Preconditions.checkArgument(KeyValuesGenerator.VALUE_BYTES_STRING.equals(fetchedValue));
            }*/
            i += _batchSize;
        }
    }
}
