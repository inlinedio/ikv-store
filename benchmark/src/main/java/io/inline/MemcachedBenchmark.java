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
    private static final String LONG_VALUE_PREFIX = "abcdefghijklmnopqrstuvwxyz";
    private static final int NUM_ENTRIES = 10_000;
    private static final int BATCH_SIZE = 100;

    public MemcachedBenchmark(MemcachedClient memcachedClient) {
        _memcachedClient = Preconditions.checkNotNull(memcachedClient);
    }

    public void initialize() {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            // Write to local client
            _memcachedClient.set(createStringKey(i), 3600, createStringValue(i));
            _memcachedClient.set(createArrayKey(i), 3600, createArrayValue(i));
        }
    }

    public void benchmarkSingleGet(int warmupIterations) {
        // warmups
        for (int iter = 0; iter < warmupIterations; iter++) {
            for (int i = 0; i < NUM_ENTRIES; i++) {
                Preconditions.checkArgument(createStringValue(i).equals(_memcachedClient.get(createStringKey(i))));
                Preconditions.checkArgument(Arrays.equals(createArrayValue(i), (int[]) _memcachedClient.get(createArrayKey(i))));
            }
        }

        // actual benchmark
        BenchmarkHistogram stringLookupBenchmarkHistogram =
                new BenchmarkHistogram("singleGet-stringLookup", NUM_ENTRIES);
        BenchmarkHistogram arrayLookupBenchmarkHistogram =
                new BenchmarkHistogram("singleGet-arrayLookup", NUM_ENTRIES);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            Instant point1 = Instant.now();

            Preconditions.checkArgument(createStringValue(i).equals(_memcachedClient.get(createStringKey(i))));

            Instant point2 = Instant.now();

            Preconditions.checkArgument(Arrays.equals(createArrayValue(i), (int[]) _memcachedClient.get(createArrayKey(i))));

            Instant point3 = Instant.now();

            stringLookupBenchmarkHistogram.setLatency(Duration.between(point1, point2).toNanos() / 1000L);
            arrayLookupBenchmarkHistogram.setLatency(Duration.between(point2, point3).toNanos() / 1000L);
        }

        System.out.println(stringLookupBenchmarkHistogram);
        System.out.println(arrayLookupBenchmarkHistogram);
    }

    public void benchmarkBatchGet(int warmupIterations) {
        for (int i = 0; i < warmupIterations; i++) {
            benchmarkBatchGetImpl();
        }

        Instant startTime = Instant.now();
        for (int i = 0; i < 1; i++) {
            benchmarkBatchGetImpl();
        }
        Duration timeTaken = Duration.between(startTime, Instant.now());

        // Print results
        String output = String.format("benchmarkBatchGet Time: %d millis, " +
                        "warmupIterations: %d, " +
                        "mainIterations: %d, " +
                        "num-entries: %d, " +
                        "batch-size: %d",
                timeTaken.toMillis(), warmupIterations, 1, NUM_ENTRIES, BATCH_SIZE);
        System.out.println(output);
    }

    private void benchmarkBatchGetImpl() {
        int i = 0;
        while (i < NUM_ENTRIES) {
            int startEntry = i;
            int endEntry = Math.min(i + BATCH_SIZE, NUM_ENTRIES);

            List<String> stringKeys = new ArrayList<>(BATCH_SIZE);
            List<String> stringValues = new ArrayList<>(BATCH_SIZE);
            List<String> arrayKeys = new ArrayList<>(BATCH_SIZE);
            List<int[]> arrayValues = new ArrayList<>(BATCH_SIZE);
            for (int j = startEntry; j < endEntry; j++) {
                stringKeys.add(createStringKey(j));
                stringValues.add(createStringValue(j));
                arrayKeys.add(createArrayKey(j));
                arrayValues.add(createArrayValue(j));
            }

            // string lookups
            Map<String, Object> results = _memcachedClient.getBulk(stringKeys);
            for (int j = 0; j < stringKeys.size(); j++) {
                String fetchedValue = (String) results.get(stringKeys.get(j));
                Preconditions.checkArgument(stringValues.get(j).equals(fetchedValue));
            }

            // array lookups
            results = _memcachedClient.getBulk(arrayKeys);
            for (int j = 0; j < arrayKeys.size(); j++) {
                int[] fetchedValue = (int[]) results.get(arrayKeys.get(j));
                Preconditions.checkArgument(Arrays.equals(arrayValues.get(j), fetchedValue));
            }

            i += BATCH_SIZE;
        }
    }

    private static String createStringKey(int id) {
        return String.format("string-key-%d", id);
    }

    private static String createStringValue(int id) {
        return String.format("%s-%d", LONG_VALUE_PREFIX, id);
    }

    private static String createArrayKey(int id) {
        return String.format("array-key-%d", id);
    }

    private static int[] createArrayValue(int id) {
        int[] arrayValue = new int[100];
        for (int j = 0; j < 100; j++) {
            arrayValue[j] = id;
        }
        return arrayValue;
    }
}
