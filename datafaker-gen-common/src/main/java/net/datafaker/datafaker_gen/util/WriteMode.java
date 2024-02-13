package net.datafaker.datafaker_gen.util;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public enum WriteMode {
    OVERWRITE(Set.of("overwrite")),
    ERROR_IF_EXISTS(Set.of("errorifexists", "error_if_exists")),
    UNDEFINED(Set.of());
    private final Set<String> identifiers;

    WriteMode(Set<String> identifiers) {
        this.identifiers = identifiers;
    }

    public static WriteMode getWriteMode(String mode) {
        if (mode == null || mode.isEmpty()) {
            return UNDEFINED;
        }

        String modeLower = mode.toLowerCase(Locale.ROOT);
        for (WriteMode value : WriteMode.values()) {
            if (value.identifiers.contains(modeLower)) {
                return value;
            }
        }

        return UNDEFINED;
    }

    public static Set<String> getLegalValues() {
        return Arrays.stream(WriteMode.values())
                .filter(writeMode -> !writeMode.equals(WriteMode.UNDEFINED))
                .map(writeMode -> String.join(", ", writeMode.identifiers))
                .collect(Collectors.toSet());
    }
}
