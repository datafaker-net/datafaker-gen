package net.datafaker.datafaker_gen.sink;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardTableDefinition;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SchemaGeneratorTest {

    @Test
    void testSchemaGenerationFromInputFunction() {
        Function<Integer, String> function = integer -> " {\"id\": 7744166, \"lastname\": \"Wunsch\", \"firstname\": \"Katherina\", \"address\": {\"country\": \"Costa Rica\", \"city\": \"West Dicktown\", \"street address\": \"48098 Jacobs Mountains\"}}";

        StandardTableDefinition schemaDefinition = SchemaGenerator.getSchema(function);

        assertNotNull(schemaDefinition);
        assertNotNull(schemaDefinition.getSchema());
        FieldList fields = schemaDefinition.getSchema().getFields();
        assertEquals(4, fields.size());
        assertEquals("firstname", fields.get(0).getName());
        assertEquals("id", fields.get(2).getName());
        assertEquals("lastname", fields.get(3).getName());
        assertEquals("address", fields.get(1).getName());

        FieldList subFields = fields.get(1).getSubFields();
        assertEquals(3, subFields.size());
        assertEquals("country", subFields.get(0).getName());
        assertEquals("city", subFields.get(1).getName());
        assertEquals("street address", subFields.get(2).getName());
    }
}