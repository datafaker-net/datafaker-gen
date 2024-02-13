package net.datafaker.datafaker_gen.sink;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommandLineSinkTest {

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @BeforeEach
    void setUp() {
        System.setOut(new PrintStream(outputStream));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }

    @Test
    void shouldPrintResultToSystemOutput() {
        CommandLineSink commandLineSink = new CommandLineSink();

        Map<String, String> sinkConfig = Map.of("batchsize", "2");
        commandLineSink.run(sinkConfig, n -> IntStream.range(0, n)
                .mapToObj(i -> String.format("Line %s", i))
                .collect(Collectors.toList()),4);

        String printedOutput = outputStream.toString().trim();
        assertEquals("[Line 0, Line 1]" + System.lineSeparator() + "[Line 0, Line 1]", printedOutput);
    }
}