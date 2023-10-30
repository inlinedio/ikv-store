package io.inline.benchmarks;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;

public class Histogram {
    private final String _name;
    private final ArrayList<Long> _data;

    public Histogram(String name, int entriesHint) {
        _name = Preconditions.checkNotNull(name);
        _data = new ArrayList<>(entriesHint);
    }

    public synchronized void captureLatency(long latency) {
        _data.add(latency);
    }

    @Override
    public String toString() {
        if (_data.size() == 0) {
            return String.format("[BenchmarkHistogram] %s no-entries", _name);
        }

        ArrayList<Long> sortedEntries = new ArrayList<>(_data);
        Collections.sort(sortedEntries);

        // avg, p50, p90, p99, pmax
        double numEntries = sortedEntries.size();
        double sum = sortedEntries.stream().reduce(0L, Long::sum);

        double average = sum/numEntries;
        double p50 = sortedEntries.get((int) (numEntries * 0.5));
        double p90 = sortedEntries.get((int) (numEntries * 0.90));
        double p99 = sortedEntries.get((int) (numEntries * 0.99));
        double pMax = sortedEntries.get((int) numEntries - 1);

        return String.format(
                "[BenchmarkHistogram] %s num-entries: %d, avg: %.2f, p50: %.2f, p90: %.2f, p99: %.2f, pMax: %.2f",
                _name,
                _data.size(),
                average,
                p50,
                p90,
                p99,
                pMax);
    }
}
