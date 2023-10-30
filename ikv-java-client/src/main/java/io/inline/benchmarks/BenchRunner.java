package io.inline.benchmarks;


public class BenchRunner {
    public static void main(String[] args) {
        // arg parsing
        String paramString = "num_entries:100000,batch_size:100";  // default
        if (args.length > 0) {
            paramString = args[0];
        }
        BenchmarkParams benchmarkParams = new BenchmarkParams(paramString);

        IKVLatencyBenchmarkWorkflow workflow = new IKVLatencyBenchmarkWorkflow(benchmarkParams);
        workflow.connect();
        Histogram histogram = new Histogram("noop", 100000);
        workflow.initializeWithWrites(histogram);
        workflow.benchmarkBatchGet(histogram);
        workflow.shutdown();

        System.out.println(histogram);

        System.exit(0);
    }
}