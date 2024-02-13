package net.datafaker.datafaker_gen.sink;

import java.util.Map;
import java.util.function.Function;

public class CommandLineSink implements Sink {
    @Override
    public String getName() {
        return "cli";
    }

    @Override
    public void run(Map<String, ?> config, Function<Integer, ?> function, int numberOfLines) {
        int numberOfLinesToPrint = numberOfLines;
        int batchSize = getBatchSize(config);
        while (numberOfLinesToPrint > 0) {
            int numberOfLinesToPrintCurrentIteration = Math.min(batchSize, numberOfLinesToPrint);
            System.out.println(function.apply(numberOfLinesToPrintCurrentIteration));
            numberOfLinesToPrint -= numberOfLinesToPrintCurrentIteration;
        }
    }
}
