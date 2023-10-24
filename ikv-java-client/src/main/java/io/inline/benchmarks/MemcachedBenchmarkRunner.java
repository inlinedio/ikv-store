package io.inline.benchmarks;

import io.inline.IKVClient;

public class MemcachedBenchmarkRunner {
    private static final String INDEX_SCHEMA = "document:\n" +
            "  - name: firstname\n" +
            "    id: 2\n" +
            "    type: string\n" +
            "  - name: age\n" +
            "    id: 0\n" +
            "    type: i32\n" +
            "  - name: profile\n" +
            "    id: 1\n" +
            "    type: bytes\n" +
            "  - name: zip\n" +
            "    id: 3\n" +
            "    type: i32";

    public static void main(String[] args) {
        // arg parsing
        String paramString = "num_entries:10000,batch_size:1000";
        if (args.length > 0) {
            paramString = args[0];
        }
        BenchmarkParams benchmarkParams = new BenchmarkParams(paramString);

        IKVClient ikvClient = IKVClient.create_new("/tmp/benchmark", INDEX_SCHEMA);

        // Local Memcached instance benchmark
        System.out.println("Starting Memcached Local benchmark");
        IKVBenchmark localBenchmark = new IKVBenchmark(ikvClient, benchmarkParams);
        localBenchmark.initialize();
        localBenchmark.benchmarkBatchGet(1);

        System.exit(0);
    }
}