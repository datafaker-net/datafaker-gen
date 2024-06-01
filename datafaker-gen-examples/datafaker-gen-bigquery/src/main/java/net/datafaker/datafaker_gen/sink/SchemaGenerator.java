package net.datafaker.datafaker_gen.sink;

import com.google.api.pathtemplate.ValidationException;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SchemaGenerator {

    private static final Map<Class<?>, StandardSQLTypeName> JSONTYPE_2_BIGQUERY_TYPE = new HashMap<>();

    static {
        JSONTYPE_2_BIGQUERY_TYPE.put(String.class, StandardSQLTypeName.STRING);
        JSONTYPE_2_BIGQUERY_TYPE.put(Integer.class, StandardSQLTypeName.INT64);
        JSONTYPE_2_BIGQUERY_TYPE.put(Long.class, StandardSQLTypeName.INT64);
        JSONTYPE_2_BIGQUERY_TYPE.put(Double.class, StandardSQLTypeName.FLOAT64);
        JSONTYPE_2_BIGQUERY_TYPE.put(Boolean.class, StandardSQLTypeName.BOOL);
        JSONTYPE_2_BIGQUERY_TYPE.put(JSONObject.class, StandardSQLTypeName.STRUCT);

        JSONTYPE_2_BIGQUERY_TYPE.put(Timestamp.class, StandardSQLTypeName.TIMESTAMP);
        JSONTYPE_2_BIGQUERY_TYPE.put(java.sql.Timestamp.class, StandardSQLTypeName.TIMESTAMP);
        JSONTYPE_2_BIGQUERY_TYPE.put(java.sql.Date.class, StandardSQLTypeName.DATE);
        JSONTYPE_2_BIGQUERY_TYPE.put(java.sql.Time.class, StandardSQLTypeName.TIME);
        JSONTYPE_2_BIGQUERY_TYPE.put(java.math.BigDecimal.class, StandardSQLTypeName.NUMERIC);
    }

    public static StandardTableDefinition getSchema(Function<Integer, ?> function) {
        JSONObject templateRecord = generateTemplateRecord(function);
        return StandardTableDefinition.newBuilder().setSchema(schemaBuilder(templateRecord)).build();
    }

    private static JSONObject generateTemplateRecord(Function<Integer, ?> function) {
        String line = (String) function.apply(1);
        return new JSONObject(line);
    }

    private static Field buildField(String fieldName, Object value) {
        StandardSQLTypeName standardSQLTypeName = JSONTYPE_2_BIGQUERY_TYPE.get(
                value instanceof JSONArray valueJSONArray ? valueJSONArray.get(0).getClass() : value.getClass()
        );
        if (standardSQLTypeName == null) {
            throw new ValidationException("Type " + value.getClass() + " is not supported");
        }

        Field.Builder fBuilder;
        if (value instanceof JSONObject
                || (value instanceof JSONArray valueJSONArray && valueJSONArray.get(0) instanceof JSONObject)
        ) {
            if (value instanceof JSONArray valueJSONArray) {
                value = valueJSONArray.getJSONObject(0);
            }
            fBuilder = Field.newBuilder(fieldName, standardSQLTypeName, buildFieldList(value));
        } else {
            fBuilder = Field.newBuilder(fieldName, standardSQLTypeName);
        }
        fBuilder.setMode(Field.Mode.NULLABLE);

        if (value instanceof JSONArray) {
            fBuilder.setMode(Field.Mode.REPEATED);
        }

        return fBuilder.build();
    }

    private static Collection<Field> buildFields(JSONObject jsonObject) {
        Collection<Field> fields = new ArrayList<>();
        for (String fieldName : jsonObject.keySet()) {
            Object value = jsonObject.get(fieldName);
            Field field = buildField(fieldName, value);
            fields.add(field);
        }
        return fields;
    }

    private static Schema schemaBuilder(JSONObject templateRecord) {
        return Schema.of(buildFields(templateRecord));
    }

    private static FieldList buildFieldList(Object value) {
        return FieldList.of(buildFields((JSONObject) value));
    }
}
