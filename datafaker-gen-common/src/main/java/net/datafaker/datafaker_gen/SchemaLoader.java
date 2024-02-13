package net.datafaker.datafaker_gen;

import net.datafaker.Faker;
import net.datafaker.shaded.snakeyaml.Yaml;
import net.datafaker.transformations.Field;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SchemaLoader {

    public static List<Field> getFields(Configuration conf) {
        final Faker faker;
        final List<Field> fields;
        final Locale defaultLocale;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(conf.getSchema()), StandardCharsets.UTF_8)) {
            final Map<String, Object> valuesMap = new Yaml().loadAs(br, Map.class);
            defaultLocale = Locale.forLanguageTag(
                    (String) Objects.requireNonNullElse(valuesMap.get("default_locale"), Locale.ENGLISH.toLanguageTag()));
            faker = new Faker(defaultLocale);
            final List<Object> list = (List<Object>) valuesMap.get("fields");
            fields = new ArrayList<>();
            for (Object o : list) {
                final Map<String, Object> stringObjectMap = (Map<String, Object>) o;
                final Field f = FieldFactory.getInstance().get(faker, stringObjectMap, defaultLocale);
                if (f != null) {
                    fields.add(f);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fields;
    }
}
