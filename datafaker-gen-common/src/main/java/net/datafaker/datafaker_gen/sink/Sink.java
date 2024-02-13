package net.datafaker.datafaker_gen.sink;

import java.util.Map;
import java.util.function.Function;

public interface Sink {
    int BATCH_SIZE = 10000;
    String getName();
    void run(Map<String, ?> config, Function<Integer, ?> function, int numberOfLines);

    default int getBatchSize(Map<String, ?> config) {
        Object batchSizeObj = config.get("batchsize");
        if (batchSizeObj == null) return BATCH_SIZE;
        if (batchSizeObj instanceof Integer) {
            return (int) batchSizeObj;
        }
        if (batchSizeObj instanceof Number) {
            return ((Number) batchSizeObj).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(batchSizeObj));
        } catch (NumberFormatException nfe) {
            return BATCH_SIZE;
        }
    }
}
