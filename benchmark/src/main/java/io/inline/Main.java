package io.inline;

import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class Main {
    public static void main(String[] args) throws IOException {
        // Local Memcached instance benchmark
        System.out.println("Starting Memcached Local benchmark");
        MemcachedClient localMemcachedClient = new MemcachedClient(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 11211));
        MemcachedBenchmark localBenchmark = new MemcachedBenchmark(localMemcachedClient);
        localBenchmark.initialize();
        localBenchmark.benchmarkSingleGet(1);
        localBenchmark.benchmarkBatchGet(1);
        localMemcachedClient.flush();

        // Remote AWS ElasticCache Memcached instance benchmark
        System.out.println("Starting Memcached Remote AWS benchmark");
        MemcachedClient remoteMemcachedClient = new MemcachedClient(
                new InetSocketAddress("pushkar-memcached-1.95dmho.cfg.eun1.cache.amazonaws.com",
                        11211));
        MemcachedBenchmark remoteBenchmark = new MemcachedBenchmark(remoteMemcachedClient);
        remoteBenchmark.initialize();
        remoteBenchmark.benchmarkSingleGet(1);
        remoteBenchmark.benchmarkBatchGet(1);
        remoteMemcachedClient.flush();
    }
}