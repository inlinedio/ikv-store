package io.inline.benchmarks;

import com.google.common.base.Preconditions;
import io.inline.IKVClient;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class IKVBenchmark {
    private final IKVClient _ikvClient;
    private final KeyValuesGenerator _keyValuesGenerator;
    private final int _numEntries;
    private final int _batchSize;


    public IKVBenchmark(IKVClient ikvClient, BenchmarkParams benchmarkParams) {
        _ikvClient = Preconditions.checkNotNull(ikvClient);
        _numEntries = benchmarkParams.getIntegerParameter("num_entries").get();
        _batchSize = benchmarkParams.getIntegerParameter("batch_size").get();
        _keyValuesGenerator = new KeyValuesGenerator(_numEntries);
    }

    // write data
    public void initialize() {
        for (int i = 0; i < _numEntries; i++) {
            String key = _keyValuesGenerator.getKey(i);
            _ikvClient.upsertFieldValue(
                    key,
                    KeyValuesGenerator.VALUE_BYTES,
                    "firstname");
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
            Collection<String> keys = _keyValuesGenerator.getKeyBatch(startEntry, endEntry);

            Instant start = Instant.now();
            Map<String, String> results = _ikvClient.getStringFieldValues(keys, "firstname");
            Instant end = Instant.now();

            benchmarkHistogram.setLatency(
                    Duration.between(start, end).toNanos() / 1000L);

            for (Map.Entry<String, String> entry: results.entrySet()) {
                String fetchedValue = entry.getValue();
                Preconditions.checkArgument(KeyValuesGenerator.VALUE_BYTES_STRING.equals(fetchedValue));
            }
            i += _batchSize;
        }
    }
}
