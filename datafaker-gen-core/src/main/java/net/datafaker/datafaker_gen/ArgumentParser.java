package net.datafaker.datafaker_gen;

import java.util.concurrent.Callable;

public class ArgumentParser {

    private ArgumentParser() {
    }

    public static Configuration parseArg(String[] args) {
        final Configuration.ConfigurationBuilder builder = Configuration.builder();
        if (args == null || args.length == 0) {
            return builder.build();
        }
        String currentConfig = "";
        for (int i = 0; i < args.length; i++) {
            var isStartConfig = args[i].startsWith("-");
            if (isStartConfig) {
                currentConfig = args[i];
            }
            final int nextI = isStartConfig ? i + 1 : i;
            switch (currentConfig) {
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
                            () -> builder.defaultFormat(args[nextI]), "Format is missed");
                    i++;
                    break;
                case "-oc":
                    CONSUMER4ARG_PARSE.accept(i < args.length - 1,
                            () -> builder.outputConf(args[nextI]), "Config for output is missed");
                    i++;
                    break;
                case "-sink":
                    CONSUMER4ARG_PARSE.accept(!isStartConfig || i < args.length - 1,
                            () -> builder.sink(args[nextI]), "Sink is missed");
                    if (isStartConfig) {
                        i++;
                    }
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

    private static void showHelp() {
        System.out.println("Help:");
        System.out.println("-f\t\tFormat to use while output");
        System.out.println("-oc\t\tConfig file for output to use");
        System.out.println("-n\t\tNumber of records to generate");
        System.out.println("-s\t\tSchema file to use");
        System.out.println("-sink\t\tOutput to use");
    }
}
