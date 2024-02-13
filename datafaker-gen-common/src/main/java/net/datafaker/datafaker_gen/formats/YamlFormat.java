package net.datafaker.datafaker_gen.formats;

import net.datafaker.transformations.YamlTransformer;

import java.util.Map;

public class YamlFormat implements Format<CharSequence> {
    @Override
    public String getName() {
        return "yaml";
    }

    @Override
    public <IN> YamlTransformer<IN> getTransformer(Map<String, String> config) {
        return new YamlTransformer<>();
    }
}
