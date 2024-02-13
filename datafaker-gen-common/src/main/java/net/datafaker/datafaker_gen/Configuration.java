package net.datafaker.datafaker_gen;

public class Configuration {
    private final int numberOfLines;
    private final String schema;
    private final String format;
    private final String outputConf;

    private final String sink;

    private Configuration(int numberOfLines, String schema, String format, String outputConf, String sink) {
        this.numberOfLines = numberOfLines;
        this.schema = schema;
        this.format = format;
        this.outputConf = outputConf;
        this.sink = sink;
    }

    public int getNumberOfLines() {
        return numberOfLines;
    }

    public String getSchema() {
        return schema;
    }

    public String getFormat() {
        return format;
    }

    public String getOutputConf() {
        return outputConf;
    }

    public String getSink() {
        return sink;
    }

    public static ConfigurationBuilder builder() {
        return new ConfigurationBuilder();
    }

    public static class ConfigurationBuilder {
        // preset default values
        private int numberOfLines = 10;
        private String schema = "config.yaml";
        private String outputConf = "output.yaml";
        private String format = "json";
        private String sink = "cli";

        private ConfigurationBuilder() {}

        public ConfigurationBuilder format(String format) {
            this.format = format;
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
            this.sink = sink;
            return this;
        }

        public Configuration build() {
            return new Configuration(numberOfLines, schema, format, outputConf, sink);
        }
    }
}
