package net.datafaker.datafaker_gen.formats;

import net.datafaker.transformations.JsonTransformer;
import net.datafaker.transformations.Transformer;

import java.util.Locale;
import java.util.Map;

public class JsonFormat implements Format<CharSequence> {
    @Override
    public String getName() {
        return "json";
    }

    @Override
    public <IN> Transformer<IN, CharSequence> getTransformer(Map<String, String> config) {
        final JsonTransformer.JsonTransformerBuilder<Object> builder = JsonTransformer.builder();
        if (config == null) {
            return (JsonTransformer<IN>) builder.build();
        }
        for (Map.Entry<String, String> entry : config.entrySet()) {
            switch (entry.getKey().toLowerCase(Locale.ROOT)) {
                case "commabetweenobjects":
                    builder.withCommaBetweenObjects(Boolean.parseBoolean(entry.getValue()));
                    break;
            }
        }
        return (JsonTransformer<IN>) builder.build();
    }
}
