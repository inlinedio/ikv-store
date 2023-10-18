package io.inline;

import com.google.common.base.Preconditions;
import net.spy.memcached.MemcachedClient;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Performance benchmark to compare Memcached -
 * local and remote (Amazon AWS ElasticCache) performance  */
public class MemcachedBenchmark {
    private final MemcachedClient _memcachedClient;
    private final KeyValuesGenerator _keyValuesGenerator;
    private final int _numEntries;
    private final int _batchSize;


    public MemcachedBenchmark(MemcachedClient memcachedClient, BenchmarkParams benchmarkParams) {
        _memcachedClient = Preconditions.checkNotNull(memcachedClient);
        _numEntries = benchmarkParams.getIntegerParameter("num_entries").get();
        _batchSize = benchmarkParams.getIntegerParameter("batch_size").get();
        _keyValuesGenerator = new KeyValuesGenerator(_numEntries);
    }

    // write data
    public void initialize() {
        for (int i = 0; i < _numEntries; i++) {
            String key = _keyValuesGenerator.getKey(i);
            _memcachedClient.set(key, 3600, KeyValuesGenerator.VALUE_BYTES);
        }
    }

    public void benchmarkSingleGet(int warmupIterations) {
        // warmups
        for (int iter = 0; iter < warmupIterations; iter++) {
            for (int i = 0; i < _numEntries; i++) {
                String key = _keyValuesGenerator.getKey(i);
                Preconditions.checkArgument(Arrays.equals(
                        KeyValuesGenerator.VALUE_BYTES, (byte[]) _memcachedClient.get(key)));
            }
        }

        // actual benchmark
        BenchmarkHistogram benchmarkHistogram =
                new BenchmarkHistogram("singleGetLookup", _numEntries);
        for (int i = 0; i < _numEntries; i++) {
            Instant start = Instant.now();
            String key = _keyValuesGenerator.getKey(i);
            Instant end = Instant.now();

            Preconditions.checkArgument(Arrays.equals(
                    KeyValuesGenerator.VALUE_BYTES, (byte[]) _memcachedClient.get(key)));

            // note latency
            benchmarkHistogram.setLatency(
                    Duration.between(start, end).toNanos() / 1000L);
        }

        System.out.println(benchmarkHistogram);
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
            Map<String, Object> results = _memcachedClient.getBulk(keys);
            Instant end = Instant.now();

            benchmarkHistogram.setLatency(
                    Duration.between(start, end).toNanos() / 1000L);

            for (String key : keys) {
                byte[] fetchedValue = (byte[]) results.get(key);
                Preconditions.checkArgument(Arrays.equals(fetchedValue, KeyValuesGenerator.VALUE_BYTES));
            }

            i += _batchSize;
        }
    }
}
