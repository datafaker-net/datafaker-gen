package net.datafaker.datafaker_gen.sink;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardTableDefinition;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaGeneratorTest {

    @Test
    void testSchemaGenerationFromInputFunction() {
        Function<Integer, String> function = integer -> " {\"id\": 7744166, \"lastname\": \"Wunsch\", \"firstname\": \"Katherina\", \"address\": {\"country\": \"Costa Rica\", \"city\": \"West Dicktown\", \"street address\": \"48098 Jacobs Mountains\"}}";

        StandardTableDefinition schemaDefinition = SchemaGenerator.getSchema(function);

        assertThat(schemaDefinition).isNotNull();
        assertThat(schemaDefinition.getSchema()).isNotNull();
        FieldList fields = schemaDefinition.getSchema().getFields();
        assertThat(fields).hasSize(4);
        assertThat(fields.get(0).getName()).isEqualTo("firstname");
        assertThat(fields.get(2).getName()).isEqualTo("id");
        assertThat(fields.get(3).getName()).isEqualTo("lastname");
        assertThat(fields.get(1).getName()).isEqualTo("address");

        FieldList subFields = fields.get(1).getSubFields();
        assertThat(subFields).hasSize(3);
        assertThat(subFields.get(0).getName()).isEqualTo("country");
        assertThat(subFields.get(1).getName()).isEqualTo("city");
        assertThat(subFields.get(2).getName()).isEqualTo("street address");
    }
}