package net.datafaker.datafaker_gen.formats;

import net.datafaker.transformations.CompositeField;
import net.datafaker.transformations.CsvTransformer;
import net.datafaker.transformations.Field;
import net.datafaker.transformations.Schema;

import java.util.Locale;
import java.util.Map;

public class CsvFormat implements Format<CharSequence> {

    @Override
    public String getName() {
        return "csv";
    }

    @Override
    public <IN> CsvTransformer<IN> getTransformer(Map<String, String> config) {
        final CsvTransformer.CsvTransformerBuilder<Object> builder = CsvTransformer.builder();
        if (config == null) {
            return (CsvTransformer<IN>) builder.build();
        }
        for (Map.Entry<String, String> entry : config.entrySet()) {
            switch (entry.getKey().toLowerCase(Locale.ROOT)) {
                case "header":
                    builder.header(Boolean.parseBoolean(entry.getValue()));
                    break;
                case "quote":
                    builder.quote(entry.getValue().charAt(0));
                    break;
                case "separator":
                    builder.separator(entry.getValue());
                    break;
            }
        }
        return (CsvTransformer<IN>) builder.build();
    }

    @Override
    public void validateSchema(Schema schema) {
        for (Field f: schema.getFields()) {
            if (f instanceof CompositeField) {
                throw new IllegalArgumentException("CSV does not support complex fields");
            }
        }
    }
}
