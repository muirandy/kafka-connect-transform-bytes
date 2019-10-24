package com.aimyourtechnology.kafka.connect.transform.bytes;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CastTest {
    private static final String SPEC_CONFIG = "spec";

    @Mock
    private Map<String, ?> sourcePartition;
    private Map<String, ?> sourceOffset;
    private String topic = "topic";
    private Integer partition = 0;
    private Transformation cast = new Cast();
    private String randomValueString = UUID.randomUUID().toString();
    private String randomKeyString = UUID.randomUUID().toString();

    @Test
    void isKafkaConnectTransformation() {
        Transformation transformation = (Transformation)cast;
    }

    @Test
    void castsSchemaValue() {
        cast.configure(Collections.singletonMap(SPEC_CONFIG, randomKeyString + ":string"));

        Schema schema = buildBaseSchema();

        ConnectRecord transformed = doTransform(schema, buildBaseStruct(schema));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        assertEquals(Schema.Type.STRING, transformed.valueSchema().field(randomKeyString).schema().type());
//        Struct transformedValue = (Struct) transformed.value();

//        assertEquals(Schema.Type.STRING, transformedValue.get("value").schema().type());
//        Object nestedValue = transformedValue.get("value");
//        cast.configure(Collections.singletonMap(SPEC_CONFIG, "int32"));
//        assertEquals(Schema.Type.INT32, doTransform(new BigInteger("42").toByteArray()).valueSchema().type());
    }

    @Test
    void castsStructValue() {
        cast.configure(Collections.singletonMap(SPEC_CONFIG, randomKeyString + ":string"));
        Schema schema = buildBaseSchema();

        ConnectRecord transformed = doTransform(schema, buildBaseStruct(schema));

        Struct resultingStruct = (Struct)transformed.value();
        Object value = resultingStruct.get(randomKeyString);
        assertEquals(String.class.getSimpleName(), value.getClass().getSimpleName());
        assertEquals(randomValueString, value.toString());
    }

    private Schema buildBaseSchema() {
        return SchemaBuilder.struct()
                .field(randomKeyString, SchemaBuilder.bytes());
    }

    private Struct buildBaseStruct(Schema schema) {
        Struct struct = new Struct(schema);
        struct.put(randomKeyString, randomValueString.getBytes());
        return struct;
    }

    private ConnectRecord doTransform(Schema schema, Struct struct) {
        return cast.apply(new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                partition,
                schema,
                struct
        ));
    }
}