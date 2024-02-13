package net.datafaker.datafaker_gen;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

class DatafakerGenTest {

    @ParameterizedTest
    @MethodSource("generateTestParameters")
    void generateTextFiles(String input, int expectedLines) throws IOException {
        DatafakerGen.main(input.split("\\s+"));

        String resultFileNameFromConfig = "res";
        Path tempFile = Paths.get(resultFileNameFromConfig);
        List<String> lines = Files.readAllLines(tempFile);
        assertThat(lines).hasSize(expectedLines);

        // Clean up
        Files.delete(tempFile);
    }

    @ParameterizedTest
    @MethodSource("generateTestParametersWithMultiSink")
    void generateTextFilesWithMultiSink(String input, List<List<String>> expectedResults) throws IOException, URISyntaxException {
        if (input.contains("-oc")) {
            int ocIndex = input.indexOf("-oc");
            int endIndex = input.indexOf(" ", ocIndex + 4);

            String outputTestConfigPath = (endIndex == -1 ? input.substring(ocIndex + 4) : input.substring(ocIndex + 4, endIndex)).trim();
            String path = Paths.get(Objects.requireNonNull(getClass().getClassLoader().getResource(outputTestConfigPath)).toURI()).toString();
            input = input.replace(outputTestConfigPath, path);
        }

        DatafakerGen.main(input.split("\\s+"));

        String resultFileNameFromConfig = "res";
        for (List<String> expectedResult : expectedResults) {
            String extantion = expectedResult.get(0);
            String expectedLines = expectedResult.get(1);
            Path tempFile = Paths.get(resultFileNameFromConfig + extantion);
            List<String> lines = Files.readAllLines(tempFile);
            assertThat(lines).hasSize(Integer.valueOf(expectedLines));

            // Clean up
            Files.delete(tempFile);
        }
    }

    private static Stream<Arguments> generateTestParameters() {
        return Stream.of(
                of("-f xml -n 10 -sink textfile -oc output.yaml", 10),
                of("-f json -n 10 -sink textfile -oc output.yaml", 12)
        );
    }

    private static Stream<Arguments> generateTestParametersWithMultiSink() {
        return Stream.of(
                of("-f xml -n 10 -oc ./outputs/output_test.yaml -sink textfile textfile:json", List.of(List.of(".xml", "10"), List.of(".json", "12"))),
                of("-f xml -n 10 -sink textfile textfile:json -oc ./outputs/output_test.yaml", List.of(List.of(".xml", "10"), List.of(".json", "12")))
        );
    }
}