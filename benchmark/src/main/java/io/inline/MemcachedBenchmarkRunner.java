package io.inline;

import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class MemcachedBenchmarkRunner {
    public static void main(String[] args) throws IOException {
        // arg parsing
        String paramString = "num_entries:10000,batch_size:500";
        if (args.length > 0) {
            paramString = args[0];
        }
        BenchmarkParams benchmarkParams = new BenchmarkParams(paramString);

        MemcachedClient localMemcachedClient = new MemcachedClient(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 11211));
        /*MemcachedClient remoteMemcachedClient = new MemcachedClient(
                new InetSocketAddress("memcached-prod.95dmho.0001.eun1.cache.amazonaws.com",
                        11211));*/

        // cleanup caches w/ flush-all
        localMemcachedClient.flush();
        // remoteMemcachedClient.flush();

        // Local Memcached instance benchmark
        System.out.println("Starting Memcached Local benchmark");
        MemcachedBenchmark localBenchmark = new MemcachedBenchmark(localMemcachedClient, benchmarkParams);
        localBenchmark.initialize();
        localBenchmark.benchmarkBatchGet(1);

        /*
        // Remote AWS ElasticCache Memcached instance benchmark
        System.out.println("Starting Memcached Remote AWS benchmark");
        MemcachedBenchmark remoteBenchmark = new MemcachedBenchmark(remoteMemcachedClient, benchmarkParams);
        remoteBenchmark.initialize();
        remoteBenchmark.benchmarkBatchGet(1);
        */

        System.exit(0);
    }
}