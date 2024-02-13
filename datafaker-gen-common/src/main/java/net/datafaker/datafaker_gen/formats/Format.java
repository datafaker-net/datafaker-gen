package net.datafaker.datafaker_gen.formats;

import net.datafaker.transformations.Schema;
import net.datafaker.transformations.Transformer;

import java.util.Map;

public interface Format<OUT> {
    String getName();
    <IN> Transformer<IN, OUT> getTransformer(Map<String, String> config);

    default void validateSchema(Schema schema) {}
}
