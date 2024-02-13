package net.datafaker.datafaker_gen;

import java.util.Map;
import java.util.Objects;

public enum FieldType {
    DEFAULT("", "name", "generators"),
    STRUCT("struct", "name", "fields"),
    ARRAY("array", "name", "minLength", "maxLength");

    private final String name;
    private final String[] requiredFields;

    FieldType(String name, String ... requiredFields) {
        this.name = name;
        this.requiredFields = requiredFields;
    }

    public static FieldType validateRequiredFieldsAndGet(Map<String, Object> map) {
        final FieldType type = of((String) map.get("type"));
        for (String field: type.requiredFields) {
            Objects.requireNonNull(map.get(field), field + " is required field for type '" + type + "'");
        }
        return type;
    }

    private static FieldType of(String value) {
        for (FieldType v: values()) {
            if (v.name.equals(value)) {
                return v;
            }
        }
        return DEFAULT;
    }
}
