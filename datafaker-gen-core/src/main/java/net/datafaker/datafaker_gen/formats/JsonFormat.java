package net.datafaker.datafaker_gen.formats;

import net.datafaker.transformations.JsonTransformer;

import java.util.Locale;
import java.util.Map;

public class JsonFormat implements Format<Object> {
    @Override
    public String getName() {
        return "json";
    }

    @Override
    public <IN> JsonTransformer<IN> getTransformer(Map<String, String> config) {
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
