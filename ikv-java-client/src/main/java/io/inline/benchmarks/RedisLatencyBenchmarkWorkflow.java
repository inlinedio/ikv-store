package io.inline.benchmarks;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class RedisLatencyBenchmarkWorkflow implements LatencyBenchmarkWorkflow {
    private final Set<HostAndPort> _jedisClusterNodes;
    private volatile JedisCluster _jedisCluster;
    // private volatile Jedis _jedis;  // for single node local benchmark

    private final KeyValuesGenerator _keyValuesGenerator;
    private final HashMap<String, byte[]> _sourceOfTruth;

    // parameters
    private final int _numEntries;
    private final int _batchSize;

    public RedisLatencyBenchmarkWorkflow(BenchmarkParams params) {
        // TODO: change initalization code.
        _jedisClusterNodes = new HashSet<>();
        _jedisClusterNodes.add(new HostAndPort("127.0.0.1", 6379));

        _numEntries = params.getIntegerParameter("num_entries").get();
        _batchSize = params.getIntegerParameter("batch_size").get();

        _keyValuesGenerator = new KeyValuesGenerator(_numEntries);
        _sourceOfTruth = Maps.newHashMapWithExpectedSize(_numEntries);
    }

    @Override
    public void connect() {
        // single node testing:
        // JedisPool jedisPool = new JedisPool("localhost", 6379);
        // _jedis = jedisPool.getResource();


        try {
            _jedisCluster = new JedisCluster(_jedisClusterNodes);
        } catch (Exception e) {
            System.out.println("Cannot connect to Redis: " + e);
            throw e;
        }
    }

    @Override
    public void initializeWithWrites(Histogram unused) {
        for (int i = 0; i < _numEntries; i++) {
            String key = _keyValuesGenerator.getKey(i);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = _keyValuesGenerator.getValueBytes(350);

            // Write to redis cluster
            _jedisCluster.set(keyBytes, valueBytes);

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

    void benchmarkSingleGetImpl(@Nullable Histogram histogram) {
        for (int i = 0; i < _numEntries; i++) {
            String key = _keyValuesGenerator.getKey(i);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = _sourceOfTruth.get(key);

            // redis lookup
            Instant start = Instant.now();
            byte[] returnedValueBytes = _jedisCluster.get(keyBytes);
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

    void benchmarkBatchGetImpl(@Nullable Histogram histogram) {
        int i = 0;
        while (i < _numEntries) {
            int startEntry = i;
            int endEntry = Math.min(i + _batchSize, _numEntries);
            List<String> stringKeys = _keyValuesGenerator.getKeyBatch(startEntry, endEntry).stream().toList();
            List<byte[]> bytesKeys = stringKeys.stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).toList();
            List<byte[]> bytesValues = stringKeys.stream().map(_sourceOfTruth::get).toList();

            // redis lookup
            byte[][] redisKeys = bytesKeys.toArray(new byte[0][]);

            Instant start = Instant.now();
            List<byte[]> returnedValues = _jedisCluster.mget(redisKeys);
            Instant end = Instant.now();

            if (histogram != null) {
                histogram.captureLatency(
                        Duration.between(start, end).toNanos() / 1000L);
            }

            // assert on returned values
            for (int j = 0; j < bytesValues.size(); j++) {
                boolean isSame = Arrays.equals(bytesValues.get(j), returnedValues.get(j));
                Preconditions.checkArgument(isSame);
            }

            i += _batchSize;
        }
    }

    @Override
    public void shutdown() {
        // _jedis.close();
        _jedisCluster.close();
        _jedisCluster = null;
    }
}
