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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;

public class DatafakerGen {

    public static void main(String[] args) {
        final Configuration conf = parseArg(args);
        final Map<String, Object> outputs;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(conf.getOutputConf()), StandardCharsets.UTF_8)) {
            outputs = new Yaml().loadAs(br, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final Map<String, Object> formats = (Map<String, Object>) outputs.get("formats");

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
        final String sinkName = conf.getSink().toLowerCase(Locale.ROOT);
        final Map<String, Object> sinksFromConfig = (Map<String, Object>) outputs.get("sinks");
        final Map<String, String> sinkConf = (Map<String, String>) sinksFromConfig.get(sinkName);
        final Sink sink = name2sink.get(sinkName);
        Objects.requireNonNull(sink,
                "Sink '" + conf.getSink() + "' is not available. The list of available sinks: " + name2sink.keySet());

        final List<Field> fields = SchemaLoader.getFields(conf);
        final Schema schema = Schema.of(fields.toArray(new Field[0]));
        sink.run(sinkConf,
                n -> findAndValidateTransformerByName(conf.getFormat(), name2Format, formats, schema)
                        .generate(schema, n), conf.getNumberOfLines());
    }


    @FunctionalInterface
    private static interface TriConsumer<A, B, C> {
        void accept(A a, B b, C c);

    }
    private static final TriConsumer<Boolean, Callable<?>, String> CONSUMER4ARG_PARSE = (aBoolean, callable, s) -> {
        if (aBoolean) {
            try {
                callable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            System.err.println(s);
            System.exit(1);
        }
    };

    public static Configuration parseArg(String[] args) {
        final Configuration.ConfigurationBuilder builder = Configuration.builder();
        if (args == null || args.length == 0) {
            return builder.build();
        }
        for (int i = 0; i < args.length; i++) {
            final int nextI = i + 1;
            switch (args[i]) {
                case "-n":
                    CONSUMER4ARG_PARSE.accept(i < args.length - 1,
                            () -> builder.numberOfLines(Integer.parseInt(args[nextI])), "Number of lines missed");
                    i++;
                    break;
                case "-s":
                    CONSUMER4ARG_PARSE.accept(i < args.length - 1,
                            () -> builder.schema(args[nextI]), "Schema file missed");
                    i++;
                    break;
                case "-f":
                    CONSUMER4ARG_PARSE.accept(i < args.length - 1,
                            () -> builder.format(args[nextI]), "Format is missed");
                    i++;
                    break;
                case "-oc":
                    CONSUMER4ARG_PARSE.accept(i < args.length - 1,
                            () -> builder.outputConf(args[nextI]), "Config for output is missed");
                    i++;
                    break;
                case "-sink":
                    CONSUMER4ARG_PARSE.accept(i < args.length - 1,
                            () -> builder.sink(args[nextI]), "Sink is missed");
                    i++;
                    break;
                case "--help":
                case "-h":
                    showHelp();
                    System.exit(0);
                default:
                    System.err.println("Unknown arg '" + args[i] + "'");
                    System.out.println();
                    showHelp();
                    System.exit(1);
            }
        }
        return builder.build();
    }

    private static void showHelp() {
        System.out.println("Help:");
        System.out.println("-f\t\tFormat to use while output");
        System.out.println("-oc\t\tConfig file for output to use");
        System.out.println("-n\t\tNumber of records to generate");
        System.out.println("-s\t\tSchema file to use");
        System.out.println("-sink\t\tOutput to use");
    }

    private static Transformer<?, ?> findAndValidateTransformerByName(String formatName,
                                                                      Map<String, Format> name2Format,
                                                                      final Map<String, Object> formatConf, Schema schema) {
        final String formatNameUpper = formatName.toUpperCase(Locale.ROOT);
        final Format format = name2Format.get(formatNameUpper);
        if (format != null) {
            format.validateSchema(schema);
            return format.getTransformer((Map<String, String>) formatConf.get(format.getName()));
        }

        var errorMessage = "'" + formatName + "'" + " is not supported yet. Available formats: ["
                + String.join(", ", name2Format.keySet()) + "]";
        throw new IllegalArgumentException(errorMessage);
    }
}
