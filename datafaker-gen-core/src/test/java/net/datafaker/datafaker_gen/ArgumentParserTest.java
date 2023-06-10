package net.datafaker.datafaker_gen;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

class ArgumentParserTest {

    @ParameterizedTest
    @MethodSource("generateTestParseArgs")
    void shouldBuildConfigurationFromInputCommandLine(String input, List<String> expectedResult) {
        Configuration configuration = ArgumentParser.parseArg(input.split("\\s+"));

        assertThat(configuration.getDefaultFormat()).isEqualTo(expectedResult.get(0));
        assertThat(configuration.getNumberOfLines()).isEqualTo(Integer.valueOf(expectedResult.get(1)));
        assertThat(String.join(" ", configuration.getSinks())).isEqualTo(expectedResult.get(2));
        assertThat(configuration.getSchema()).isEqualTo(expectedResult.get(3));
        assertThat(configuration.getOutputConf()).isEqualTo(expectedResult.get(4));
    }

    private static Stream<Arguments> generateTestParseArgs() {
        return Stream.of(
                of("-n 10", List.of("json", "10", "cli", "config.yaml", "output.yaml")),
                of("-n 10 -sink cli:xml", List.of("json", "10", "cli:xml", "config.yaml", "output.yaml")),
                of("-f xml -n 10 -sink cli", List.of("xml", "10", "cli", "config.yaml", "output.yaml")),
                of("-f xml -n 10 -sink cli -oc test.yaml", List.of("xml", "10", "cli", "config.yaml", "test.yaml")),
                of("-f xml -n 10 -oc test.yaml -sink textfile textfile:json", List.of("xml", "10", "textfile textfile:json", "config.yaml", "test.yaml")),
                of("-f xml -n 10 -sink textfile textfile:json -oc test.yaml", List.of("xml", "10", "textfile textfile:json", "config.yaml", "test.yaml")),
                of("-f xml -n 10 -sink textfile textfile:json textfile:yaml -oc test.yaml", List.of("xml", "10", "textfile textfile:json textfile:yaml", "config.yaml", "test.yaml"))
        );
    }
}