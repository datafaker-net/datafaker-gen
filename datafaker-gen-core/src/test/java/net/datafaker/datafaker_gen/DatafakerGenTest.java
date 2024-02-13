package net.datafaker.datafaker_gen;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

class DatafakerGenTest {

    @ParameterizedTest
    @MethodSource("generateTestParameters")
    void generateJsonWith10LineAndFileOutput(String input, int expectedLines) throws IOException {
        DatafakerGen.main(input.split("\\s+"));

        String resultFileNameFromConfig = "res";
        Path tempFile = Paths.get(resultFileNameFromConfig);
        List<String> lines = Files.readAllLines(tempFile);
        assertThat(lines).hasSize(expectedLines);

        // Clean up
        Files.delete(tempFile);
    }

    @ParameterizedTest
    @MethodSource("generateTestParseArgs")
    void parseArgWhenFormatJsonAndTenLinesAndSinkCliTest(String input, List<String> expectedResult) {
        Configuration configuration = DatafakerGen.parseArg(input.split("\\s+"));

        assertThat(configuration.getFormat()).isEqualTo(expectedResult.get(0));
        assertThat(configuration.getNumberOfLines()).isEqualTo(Integer.valueOf(expectedResult.get(1)));
        assertThat(configuration.getSink()).isEqualTo(expectedResult.get(2));
        assertThat(configuration.getSchema()).isEqualTo(expectedResult.get(3));
        assertThat(configuration.getOutputConf()).isEqualTo(expectedResult.get(4));
    }

    private static Stream<Arguments> generateTestParseArgs() {
        return Stream.of(
                of("-f xml -n 10 -sink cli", List.of("xml", "10", "cli", "config.yaml", "output.yaml")),
                of("-f xml -n 10 -sink cli -oc test.yaml", List.of("xml", "10", "cli", "config.yaml", "test.yaml"))
        );
    }

    private static Stream<Arguments> generateTestParameters() {
        return Stream.of(
                of("-f xml -n 10 -sink textfile -oc output.yaml", 10),
                of("-f json -n 10 -sink textfile -oc output.yaml", 12)
        );
    }
}