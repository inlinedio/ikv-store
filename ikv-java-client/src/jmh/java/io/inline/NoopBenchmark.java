package io.inline;

import com.google.common.collect.Comparators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
public class NoopBenchmark {
    @Benchmark
    public void sorting(Blackhole bh) {
        List<Integer> list = IntStream.range(0, 100).boxed().toList();
        list = new ArrayList<>(list);
        Collections.sort(list);
        bh.consume(list);
    }
}
