package net.datafaker.datafaker_gen.formats;

import net.datafaker.transformations.XmlTransformer;

import java.util.Locale;
import java.util.Map;

public class XmlFormat implements Format<CharSequence> {
    @Override
    public String getName() {
        return "xml";
    }

    @Override
    public <IN> XmlTransformer<IN> getTransformer(Map<String, String> config) {
        final XmlTransformer.XmlTransformerBuilder<Object> builder = new XmlTransformer.XmlTransformerBuilder<>();
        if (config == null) {
            return (XmlTransformer<IN>) builder.build();
        }
        for (Map.Entry<String, String> entry : config.entrySet()) {
            switch (entry.getKey().toLowerCase(Locale.ROOT)) {
                case "pretty":
                    final Object isPretty = entry.getValue();
                    if (isPretty instanceof Boolean) {
                        builder.pretty((Boolean) isPretty);
                    } else {
                        builder.pretty(Boolean.parseBoolean(entry.getValue()));
                    }
                    break;
            }
        }
        return (XmlTransformer<IN>) builder.build();
    }
}
