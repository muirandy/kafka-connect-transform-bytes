package com.aimyourtechnology.kafka.connect.transform.bytes;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.List;
import java.util.Map;

public class Cast<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String SPEC_CONFIG = "spec";

    private Map<String, ?> configuration;

    @Override
    public R apply(R record) {
        Schema modifiedSchema = buildModifiedSchema(record);
        return record.newRecord(null, null, null, null, modifiedSchema, buildModifiedStruct(record, modifiedSchema),
                record.timestamp());
    }

    private Struct buildModifiedStruct(R record, Schema modifiedSchema) {
//        R newRecord = record.newRecord(record.topic(),
//                record.kafkaPartition(),
//                record.keySchema(),
//                record.key(),
//                modifiedSchema,
//                record.value(),
//                record.timestamp());
        Struct baseStruct = (Struct) record.value();

        List<Field> fields = modifiedSchema.fields();
        Struct modifiedStruct = null;
        for (Field f : fields) {
            modifiedStruct = buildModifiedStructValue(modifiedSchema, baseStruct, f.name());
        }

        return modifiedStruct;
    }

    private Struct buildModifiedStructValue(Schema modifiedSchema, Struct baseStruct, String fieldName) {
        byte[] value = (byte[]) baseStruct.get(fieldName);
        String valueAsString = new String(value);

        return new Struct(modifiedSchema).put(fieldName, valueAsString);
    }

    private Schema buildModifiedSchema(R record) {
        return buildBaseSchema(record.valueSchema());
//        String s = readConfiguredSpec().toString();
//        return SchemaBuilder.struct();
//        return record.valueSchema();
    }

    private Schema buildBaseSchema(Schema originalSchema) {

        SchemaBuilder modifiedSchema = SchemaBuilder.struct();

        for (Field f : originalSchema.fields()) {
            modifiedSchema = modifiedSchema.field(f.name(), SchemaBuilder.string());
        }

        return modifiedSchema;
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
    }

}
