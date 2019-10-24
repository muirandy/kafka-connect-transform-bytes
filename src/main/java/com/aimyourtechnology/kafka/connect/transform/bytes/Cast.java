package com.aimyourtechnology.kafka.connect.transform.bytes;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class Cast<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String SPEC_CONFIG = "spec";

    private Map<String, ?> configuration;
    private Map<String, String> casts;

    @Override
    public R apply(R record) {
        Schema modifiedSchema = buildModifiedSchema(record);
        return record.newRecord(null, null, null, null, modifiedSchema, buildModifiedStruct(record, modifiedSchema),
                record.timestamp());
    }

    private Schema buildModifiedSchema(R record) {
        Schema originalSchema = record.valueSchema();
        return new SchemaRebuilder(originalSchema, casts).buildBaseSchema();
    }

    private Struct buildModifiedStruct(R record, Schema modifiedSchema) {
        Struct originalStruct = (Struct) record.value();
        return new StructRebuilder(modifiedSchema, originalStruct, casts).modifyStruct();
    }

    private Object castBytesToString(Struct baseStruct, String fieldName) {
        byte[] value = (byte[]) baseStruct.get(fieldName);
        return new String(value);
    }

    private SchemaBuilder convertFieldType(Schema.Type type) {
        switch (type) {
            case INT8:
                return SchemaBuilder.int8();
            case INT16:
                return SchemaBuilder.int16();
            case INT32:
                return SchemaBuilder.int32();
            case INT64:
                return SchemaBuilder.int64();
            case FLOAT32:
                return SchemaBuilder.float32();
            case FLOAT64:
                return SchemaBuilder.float64();
            case BOOLEAN:
                return SchemaBuilder.bool();
            case STRING:
                return SchemaBuilder.string();
            default:
                throw new DataException("Unexpected type in Cast transformation: " + type);
        }

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        configuration = map;
        String spec = (String) configuration.get(SPEC_CONFIG);
        String[] casts = spec.split(",");
        this.casts = Arrays.stream(casts)
                .collect(Collectors.toMap(
                        c -> (c.split(":"))[0],
                        c -> (c.split(":"))[1]
                ));
    }

    private class SchemaRebuilder {
        private Schema originalSchema;
        private Map<String, String> casts;

        SchemaRebuilder(Schema originalSchema, Map<String, String> casts) {
            this.originalSchema = originalSchema;
            this.casts = casts;
        }

        Schema buildBaseSchema() {
            SchemaBuilder modifiedSchema = SchemaBuilder.struct();

            for (Field f : originalSchema.fields())
                if (Schema.Type.STRUCT.equals(f.schema().type())) {
                    Schema originalNestedSchema = schema(f.name(), originalSchema);
                    Map<String, String> nestedCasts = calculateNestedCasts(f.name(), casts);
                    modifiedSchema = modifiedSchema.field(f.name(), new SchemaRebuilder(originalNestedSchema, nestedCasts).buildBaseSchema());
                } else
                    modifiedSchema = modifiedSchema.field(f.name(), schema(f.name(), originalSchema));

            return modifiedSchema.build();
        }

        private Schema schema(String name, Schema originalSchema) {
            if (castExistsFor(name))
                return SchemaBuilder.string();
            return originalSchema.field(name).schema();
        }

        private Map<String, String> calculateNestedCasts(String name, Map<String, String> casts) {
            return casts.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(name + "."))
                    .collect(Collectors.toMap(
                            e -> e.getKey().substring(e.getKey().indexOf(".") + 1),
                            e -> e.getValue()
                    ));
        }

        private boolean castExistsFor(String key) {
            return this.casts.containsKey(key);
        }

    }

    private class StructRebuilder {
        private Schema modifiedSchema;
        private Struct originalStruct;
        private Map<String, String> casts;

        StructRebuilder(Schema modifiedSchema, Struct originalStruct, Map<String, String> casts) {
            this.modifiedSchema = modifiedSchema;
            this.originalStruct = originalStruct;
            this.casts = casts;
        }

        Struct modifyStruct() {
            Struct modifiedStruct = new Struct(modifiedSchema);

            for (Field f : modifiedSchema.fields()) {
                if (Schema.Type.STRUCT.equals(f.schema().type())) {
                    Struct nestedStruct = (Struct)originalStruct.get(f.name());
                    modifiedStruct.put(f.name(), new StructRebuilder(f.schema(), nestedStruct, calculateNestedCasts(f.name(), this.casts)).modifyStruct());
                } else
                    modifiedStruct.put(f.name(), buildModifiedStructValue(originalStruct, f.name()));
            }

            return modifiedStruct;
        }

        private Object buildModifiedStructValue(Struct baseStruct, String fieldName) {
            if (castExistsFor(fieldName))
                return castBytesToString(baseStruct, fieldName);

            return baseStruct.get(fieldName);
        }

        private Map<String, String> calculateNestedCasts(String name, Map<String, String> casts) {
            return casts.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(name + "."))
                    .collect(Collectors.toMap(
                            e -> e.getKey().substring(e.getKey().indexOf(".") + 1),
                            e -> e.getValue()
                    ));
        }

        private boolean castExistsFor(String key) {
            return this.casts.containsKey(key);
        }

    }
}