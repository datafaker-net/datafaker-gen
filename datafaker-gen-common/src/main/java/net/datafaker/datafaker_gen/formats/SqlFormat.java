package net.datafaker.datafaker_gen.formats;

import net.datafaker.transformations.sql.SqlTransformer;

import java.util.Locale;
import java.util.Map;

public class SqlFormat implements Format<CharSequence> {
    @Override
    public String getName() {
        return "sql";
    }

    @Override
    public <IN> SqlTransformer<IN> getTransformer(Map<String, String> config) {
        final SqlTransformer.SqlTransformerBuilder<Object> builder = SqlTransformer.builder();
        if (config == null) {
            return (SqlTransformer<IN>) builder.build();
        }
        for (Map.Entry<String, String> entry : config.entrySet()) {
            switch (entry.getKey().toLowerCase(Locale.ROOT)) {
                case "tablename":
                    builder.tableName(entry.getValue());
                    break;
                case "quote":
                    builder.quote(entry.getValue().charAt(0));
                    break;
                case "schemaname":
                    builder.schemaName(entry.getValue());
                    break;
                case "sqlidentifierquote":
                    builder.sqlQuoteIdentifier(entry.getValue());
                    break;
                case "batch":
                    Object batch = entry.getValue();
                    if (batch instanceof Number) {
                        int value = ((Number) batch).intValue();
                        if (value > 0) {
                            builder.batch(value);
                        } else {
                            builder.batch();
                        }
                    }
                    if (batch == null || String.valueOf(batch).trim().isEmpty()) {
                        builder.batch();
                    } else {
                        builder.batch(Integer.parseInt(String.valueOf(batch)));
                    }
                    break;
            }
        }
        return (SqlTransformer<IN>) builder.build();
    }
}
