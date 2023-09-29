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

    // add 1K entries, with 1hr expiration time.
    public void initialize() {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            // Write to local client
            _memcachedClient.set(createStringKey(i), 3600, createStringValue(i));
            _memcachedClient.set(createArrayKey(i), 3600, createArrayValue(i));
        }
    }

    public void benchmarkSingleGet(int warmupIterations, int mainIterations) {
        for (int i = 0; i < warmupIterations; i++) {
            benchmarkSingleGetImpl();
        }

        Instant startTime = Instant.now();
        for (int i = 0; i < mainIterations; i++) {
            benchmarkSingleGetImpl();
        }
        Duration timeTaken = Duration.between(startTime, Instant.now());

        // Print results
        String output = String.format("benchmarkSingleGet Time: %d millis, " +
                "warmupIterations: %d, " +
                "mainIterations: %d, " +
                "num-entries: %d", timeTaken.toMillis(), warmupIterations, mainIterations, NUM_ENTRIES);
        System.out.println(output);
    }

    private void benchmarkSingleGetImpl() {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            Preconditions.checkArgument(createStringValue(i).equals(_memcachedClient.get(createStringKey(i))));
            Preconditions.checkArgument(Arrays.equals(createArrayValue(i), (int[]) _memcachedClient.get(createArrayKey(i))));
        }
    }

    public void benchmarkBatchGet(int warmupIterations, int mainIterations) {
        for (int i = 0; i < warmupIterations; i++) {
            benchmarkBatchGetImpl();
        }

        Instant startTime = Instant.now();
        for (int i = 0; i < mainIterations; i++) {
            benchmarkBatchGetImpl();
        }
        Duration timeTaken = Duration.between(startTime, Instant.now());

        // Print results
        String output = String.format("benchmarkBatchGet Time: %d millis, " +
                        "warmupIterations: %d, " +
                        "mainIterations: %d, " +
                        "num-entries: %d, " +
                        "batch-size: %d",
                timeTaken.toMillis(), warmupIterations, mainIterations, NUM_ENTRIES, BATCH_SIZE);
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
