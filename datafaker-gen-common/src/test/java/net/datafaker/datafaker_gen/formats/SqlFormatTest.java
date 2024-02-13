package net.datafaker.datafaker_gen.formats;

import net.datafaker.providers.base.BaseFaker;
import net.datafaker.transformations.Schema;
import net.datafaker.transformations.sql.SqlTransformer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Random;

import static net.datafaker.transformations.Field.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlFormatTest {

    @Test
    void shouldGenerateSqlBasedOnSchemaWithTableNameAndSchemaName() {
        BaseFaker faker = new BaseFaker(new Random(10L));

        Schema<Object, ?> schema = Schema.of(
                field("Text", () -> faker.name().firstName()),
                field("Bool", () -> faker.bool().bool())
        );

        String expected = "INSERT INTO \"my_schema\".\"test_table\" (\"Text\", \"Bool\") VALUES ('Willis', false);";

        SqlFormat sqlFormat = new SqlFormat();

        Map<String, String> config = Map.of(
                "tablename", "test_table",
                "schemaname", "my_schema"
        );
        SqlTransformer<Object> transformer = sqlFormat.getTransformer(config);
        String result = transformer.generate(schema, 1);
        assertEquals(expected, result);
    }

    @Test
    void shouldGenerateSqlWithCustomQuoteConfig() {
        BaseFaker faker = new BaseFaker(new Random(10L));

        Schema<Object, ?> schema = Schema.of(
                field("Text", () -> faker.name().firstName()),
                field("Bool", () -> faker.bool().bool())
        );

        SqlFormat sqlFormat = new SqlFormat();
        Map<String, String> config = Map.of(
                "tablename", "test_table",
                "schemaname", "my_schema",
                "quote", "\"",
                "sqlidentifierquote", "\'\'"
        );
        SqlTransformer<Object> transformer = sqlFormat.getTransformer(config);
        String result = transformer.generate(schema, 1);

        String expected = "INSERT INTO 'my_schema'.'test_table' ('Text', 'Bool') VALUES (\"Willis\", false);";
        assertEquals(expected, result);
    }
}