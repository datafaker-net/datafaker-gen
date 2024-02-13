package net.datafaker.datafaker_gen;

import java.util.ArrayList;
import java.util.List;

public class Configuration {
    private final int numberOfLines;
    private final String schema;
    private final String format;
    private final String outputConf;

    private final List<String> sinks;

    private Configuration(int numberOfLines, String schema, String format, String outputConf, List<String> sinks) {
        this.numberOfLines = numberOfLines;
        this.schema = schema;
        this.format = format;
        this.outputConf = outputConf;
        this.sinks = sinks;
    }

    public int getNumberOfLines() {
        return numberOfLines;
    }

    public String getSchema() {
        return schema;
    }

    public String getDefaultFormat() {
        return format;
    }

    public String getOutputConf() {
        return outputConf;
    }

    public List<String> getSinks() {
        return sinks;
    }

    public static ConfigurationBuilder builder() {
        return new ConfigurationBuilder();
    }

    public static class ConfigurationBuilder {
        // preset default values
        private int numberOfLines = 10;
        private String schema = "config.yaml";
        private String outputConf = "output.yaml";
        private String defaultFormat = "json";
        private List<String> sinks = new ArrayList<>();

        private ConfigurationBuilder() {}

        public ConfigurationBuilder defaultFormat(String format) {
            this.defaultFormat = format;
            return this;
        }

        public ConfigurationBuilder outputConf(String outputConf) {
            this.outputConf = outputConf;
            return this;
        }

        public ConfigurationBuilder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public ConfigurationBuilder numberOfLines(int numberOfLines) {
            this.numberOfLines = numberOfLines;
            return this;
        }

        public ConfigurationBuilder sink(String sink) {
            this.sinks.add(sink);
            return this;
        }

        public Configuration build() {
            if (sinks.isEmpty()) sinks = List.of("cli");
            return new Configuration(numberOfLines, schema, defaultFormat, outputConf, sinks);
        }
    }
}
