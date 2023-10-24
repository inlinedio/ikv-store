package io.inline.benchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BenchmarkParams {
    private final Map<String, String> _params;

    public BenchmarkParams(String input) {
        // input format= "key1:value1,key2:value2,key3:value3"
        _params = new HashMap<>();

        String[] pairs = input.split(",");
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            _params.put(keyValue[0], keyValue[1]);
        }
    }

    public Optional<String> getStringParameter(String key) {
        return Optional.ofNullable(_params.get(key));
    }

    public Optional<Integer> getIntegerParameter(String key) {
        String value = _params.get(key);
        if (value == null) {
            return Optional.empty();
        }

        return Optional.of(Integer.valueOf(value));
    }
}
