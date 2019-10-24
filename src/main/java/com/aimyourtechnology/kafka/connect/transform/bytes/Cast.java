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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Cast<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String SPEC_CONFIG = "spec";

    private Map<String, ?> configuration;
    private Map<String,String> casts;

    @Override
    public R apply(R record) {
        Schema modifiedSchema = buildModifiedSchema(record);
        return record.newRecord(null, null, null, null, modifiedSchema, buildModifiedStruct(record, modifiedSchema),
                record.timestamp());
    }

    private Struct buildModifiedStruct(R record, Schema modifiedSchema) {
        Struct baseStruct = (Struct) record.value();

        List<Field> fields = modifiedSchema.fields();
        Struct modifiedStruct = new Struct(modifiedSchema);
        for (Field f : fields)
            modifiedStruct.put(f.name(), buildModifiedStructValue(baseStruct, f.name()));

        return modifiedStruct;
    }

    private Object buildModifiedStructValue(Struct baseStruct, String fieldName) {
        if (castExistsFor(fieldName)) {
            byte[] value = (byte[]) baseStruct.get(fieldName);
            String valueAsString = new String(value);
            return valueAsString;
        }

        return baseStruct.get(fieldName);
    }

    private Schema buildModifiedSchema(R record) {
        return buildBaseSchema(record.valueSchema());
    }

    private Schema buildBaseSchema(Schema originalSchema) {

        SchemaBuilder modifiedSchema = SchemaBuilder.struct();

        for (Field f : originalSchema.fields()) {
            if (castExistsFor(f.name()))
                modifiedSchema = modifiedSchema.field(f.name(), SchemaBuilder.string());
            else
                modifiedSchema = modifiedSchema.field(f.name(), originalSchema.field(f.name()).schema());
        }

        return modifiedSchema;
    }

    private boolean castExistsFor(String key) {
        return casts.containsKey(key);
    }

    private Object readConfiguredSpec() {
        return configuration.get(SPEC_CONFIG);
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

}
