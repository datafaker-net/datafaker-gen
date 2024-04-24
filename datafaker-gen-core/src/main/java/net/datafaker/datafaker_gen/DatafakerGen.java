package net.datafaker.datafaker_gen;

import net.datafaker.datafaker_gen.formats.Format;
import net.datafaker.datafaker_gen.sink.Sink;
import net.datafaker.shaded.snakeyaml.Yaml;
import net.datafaker.transformations.Field;
import net.datafaker.transformations.Schema;
import net.datafaker.transformations.Transformer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public class DatafakerGen {

    public static void main(String[] args) {
        final Configuration conf = ArgumentParser.parseArg(args);
        final Map<String, Object> outputs;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(conf.getOutputConf()), StandardCharsets.UTF_8)) {
            outputs = new Yaml().loadAs(br, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final ServiceLoader<Format> fs = ServiceLoader.load(Format.class);
        final Map<String, Format> name2Format = new HashMap<>();
        for (Format<?> f : fs) {
            name2Format.put(
                    f.getName().toUpperCase(Locale.ROOT), f);
        }

        final ServiceLoader<Sink> sinks = ServiceLoader.load(Sink.class);
        final Map<String, Sink> name2sink = new HashMap<>();
        for (Sink s : sinks) {
            name2sink.put(s.getName().toLowerCase(Locale.ROOT), s);
        }
        validateSinks(conf, name2sink);

        final List<Field> fields = SchemaLoader.getFields(conf);
        final Schema schema = Schema.of(fields.toArray(new Field[0]));
        conf.getSinks().forEach((sinkConfigName) -> {
            SinkToFormat sink2FormatBySinkConfigName = getSink2FormatByConfigName(sinkConfigName, conf.getDefaultFormat());

            Map<String, String> sinkOutputConfig = getSinkOutputConfig(outputs, sink2FormatBySinkConfigName);
            Map<String, Object> formatOutputConfig = getFormatOutputConfig(outputs);
            final Map<String, String> config = new HashMap<>(sinkOutputConfig);
            config.putAll(getFromatConfig(formatOutputConfig, sink2FormatBySinkConfigName.getFormatName()));

            final Sink sink = name2sink.get(sink2FormatBySinkConfigName.getSinkName());
            sink.run(config,
                    n -> findAndValidateTransformerByName(sink2FormatBySinkConfigName.getFormatName(), name2Format, formatOutputConfig, schema)
                            .generate(schema, n), conf.getNumberOfLines());
        });

    }

    private static Map<String, Object> getFormatOutputConfig(Map<String, Object> outputs) {
        final Map<String, Object> formats = (Map<String, Object>) outputs.get("formats");
        return formats;
    }

    private static Map<String, String> getSinkOutputConfig(Map<String, Object> outputs, SinkToFormat sink2FormatByConfigName) {
        final Map<String, Object> sinksFromConfig = (Map<String, Object>) outputs.get("sinks");
        Map<String, String> sinkOutputConfig = (Map<String, String>) sinksFromConfig.get(sink2FormatByConfigName.getSinkName());
        return sinkOutputConfig;
    }

    private static SinkToFormat getSink2FormatByConfigName(String sinkConfigName, String defaultFormat) {
        if (sinkConfigName.contains(":")) {
            String[] sinkFormat = sinkConfigName.split(":");
            final String sinkName = sinkFormat[0].toLowerCase(Locale.ROOT);
            return new SinkToFormat(sinkName, sinkFormat[1]);
        } else {
            final String sinkName = sinkConfigName.toLowerCase(Locale.ROOT);
            return new SinkToFormat(sinkName, defaultFormat);
        }
    }

    private static void validateSinks(Configuration conf, Map<String, Sink> name2sink) {
        conf.getSinks().forEach((sinkConfigName) -> {
            var sinkName = sinkConfigName.contains(":") ? sinkConfigName.split(":")[0] : sinkConfigName;
            final Sink sink = name2sink.get(sinkName);
            Objects.requireNonNull(sink,
                    "Sink '" + conf.getSinks() + "' is not available. The list of available sinks: " + name2sink.keySet());
        });
    }

    private static Transformer<?, ?> findAndValidateTransformerByName(String formatName,
                                                                      Map<String, Format> name2Format,
                                                                      final Map<String, Object> formatConf, Schema schema) {
        final String formatNameUpper = formatName.toUpperCase(Locale.ROOT);
        final Format format = name2Format.get(formatNameUpper);
        if (format != null) {
            format.validateSchema(schema);
            return format.getTransformer(getFromatConfig(formatConf, format.getName()));
        }

        var errorMessage = "'" + formatName + "'" + " is not supported yet. Available formats: ["
                + String.join(", ", name2Format.keySet()) + "]";
        throw new IllegalArgumentException(errorMessage);
    }

    private static Map<String, String> getFromatConfig(Map<String, Object> formatConf, String formatName) {
        return Objects.requireNonNullElse((Map<String, String>) formatConf.get(formatName), Collections.emptyMap());
    }

    private static class SinkToFormat {
        private final String sinkName;
        private final String formatName;

        public SinkToFormat(String sinkName, String formatName) {
            this.sinkName = sinkName;
            this.formatName = formatName;
        }

        public String getSinkName() {
            return sinkName;
        }

        public String getFormatName() {
            return formatName;
        }
    }
}
