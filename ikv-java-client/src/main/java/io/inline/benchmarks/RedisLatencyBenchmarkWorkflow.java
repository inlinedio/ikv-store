package io.inline.benchmarks;

import com.google.common.base.Preconditions;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class RedisLatencyBenchmarkWorkflow implements LatencyBenchmarkWorkflow {

    // Usage:
    // java -Xms10g -Xmx10g -cp ikv-java-client-redis-single.jar io.inline.benchmarks.RedisLatencyBenchmarkWorkflow "num_entries:10000,batch_size:100"
    public static void main(String[] args) {
        // arg parsing
        String paramString = "num_entries:100000,batch_size:100";  // default
        if (args.length > 0) {
            paramString = args[0];
        }
        BenchmarkParams benchmarkParams = new BenchmarkParams(paramString);

        RedisLatencyBenchmarkWorkflow workflow = new RedisLatencyBenchmarkWorkflow(benchmarkParams);
        workflow.connect();
        Histogram histogram = new Histogram("RedisBenchmarks", 100000);
        workflow.initializeWithWrites(histogram);
        workflow.benchmarkBatchGet(histogram);
        workflow.shutdown();

        System.out.println(histogram);
        System.exit(0);
    }

    private final Set<HostAndPort> _jedisClusterNodes;
    private volatile JedisCluster _jedisCluster;
    // private volatile Jedis _jedis;  // for single node local benchmark

    private final KeyValuesGenerator _keyValuesGenerator;
    private final ConcurrentHashMap<KeyValuesGenerator.BytesKey, byte[]> _sourceOfTruth;

    // parameters
    private final int _numEntries;
    private final int _batchSize;

    public RedisLatencyBenchmarkWorkflow(BenchmarkParams params) {
        _jedisClusterNodes = new HashSet<>();
        _jedisClusterNodes.add(new HostAndPort("main-cluster.95dmho.clustercfg.memorydb.eu-north-1.amazonaws.com", 6379));

        _numEntries = params.getIntegerParameter("num_entries").get();
        _batchSize = params.getIntegerParameter("batch_size").get();

        _keyValuesGenerator = new KeyValuesGenerator(_numEntries);
        _sourceOfTruth = new ConcurrentHashMap<>(_numEntries);
    }

    @Override
    public void connect() {
        // single node testing:
        JedisPool jedisPool = new JedisPool("localhost", 6379);
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
        List<CompletableFuture<?>> futures = new ArrayList<>();

        int numThreads = 100;
        int interval = _numEntries / numThreads;

        for (int i = 0; i < _numEntries; ) {
            int start = i;
            int end = Math.min(_numEntries, start + interval);

            CompletableFuture<Void> task = CompletableFuture.runAsync(() -> write(start, end));
            futures.add(task);

            i += interval;
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        System.out.println("Writes finished!");
    }

    private void write(int start, int end) {
        for (int i = start; i < end; i++) {
            KeyValuesGenerator.BytesKey key = _keyValuesGenerator.getKey(i);
            byte[] keyBytes = key.getInnerBytes();
            byte[] valueBytes = _keyValuesGenerator.getValueBytes(350, i);

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
            KeyValuesGenerator.BytesKey key = _keyValuesGenerator.getKey(i);
            byte[] keyBytes = key.getInnerBytes();
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

    public List<byte[]> getValuesTemp(byte[][] redisKeys) {
        return _jedisCluster.mget(redisKeys);
    }

    void benchmarkBatchGetImpl(@Nullable Histogram histogram) {
        int i = 0;
        while (i < _numEntries) {
            int startEntry = i;
            int endEntry = Math.min(i + _batchSize, _numEntries);
            List<byte[]> bytesKeys = _keyValuesGenerator.getKeyBatch(startEntry, endEntry).stream()
                    .map(KeyValuesGenerator.BytesKey::getInnerBytes).toList();

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
        // _jedis.close();
        _jedisCluster.close();
        _jedisCluster = null;
    }
}
