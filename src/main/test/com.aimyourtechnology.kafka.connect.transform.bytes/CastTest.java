package com.aimyourtechnology.kafka.connect.transform.bytes;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

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
    private String randomSecondKeyString = UUID.randomUUID().toString();

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
        assertEquals("base", transformed.valueSchema().name());
        assertEquals(Schema.Type.STRING, transformed.valueSchema().field(randomKeyString).schema().type());
        assertEquals("baseRandomKey", transformed.valueSchema().field(randomKeyString).schema().name());
        assertEquals(Schema.Type.BYTES, transformed.valueSchema().field(randomSecondKeyString).schema().type());
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

    @Test
    void castsNestedSchemaValue() {
        cast.configure(Collections.singletonMap(SPEC_CONFIG, "A." + randomKeyString + ":string"));
        Schema schema = buildNestedSchema();

        ConnectRecord transformed = doTransform(schema, buildNestedStruct(schema));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        assertEquals(Schema.Type.BYTES, transformed.valueSchema().field(randomKeyString).schema().type());
        assertEquals(Schema.Type.BYTES, transformed.valueSchema().field(randomSecondKeyString).schema().type());
        Field nestedField = transformed.valueSchema().field("A");
        assertEquals(Schema.Type.STRUCT, nestedField.schema().type());
        assertEquals(Schema.Type.STRING, nestedField.schema().field(randomKeyString).schema().type());
        assertEquals(Schema.Type.BYTES, nestedField.schema().field(randomSecondKeyString).schema().type());
    }

    @Test
    void castsNestedStructValue() {
        cast.configure(Collections.singletonMap(SPEC_CONFIG, "A." + randomKeyString + ":string"));
        Schema schema = buildNestedSchema();

        ConnectRecord transformed = doTransform(schema, buildNestedStruct(schema));

        Struct resultingStruct = (Struct)transformed.value();
        Struct nestedStruct = (Struct)resultingStruct.get("A");
        assertEquals(randomValueString, nestedStruct.get(randomKeyString));
    }

    @Test
    void castsNestedStructForNull() {
        cast.configure(Collections.singletonMap(SPEC_CONFIG, "A." + randomKeyString + ":string"));
        Schema schema = buildNestedSchemaWithOptional();

        ConnectRecord transformed = doTransform(schema, buildNestedStructWithNull(schema));

        Struct resultingStruct = (Struct)transformed.value();
        Struct nestedStruct = (Struct)resultingStruct.get("A");
        assertNull(nestedStruct);
    }

    @Test
    void configValues() {
        cast.configure(Collections.singletonMap(SPEC_CONFIG, "A." + randomKeyString + ":string"));

        ConfigDef config = cast.config();

        Map<String, ConfigDef.ConfigKey> configKeys = config.configKeys();

        assertNotNull(configKeys);
        assertTrue(configKeys.containsKey(SPEC_CONFIG));
        ConfigDef.ConfigKey configKey = configKeys.get(SPEC_CONFIG);
        assertEquals(ConfigDef.Type.LIST, configKey.type);
        assertEquals(ConfigDef.NO_DEFAULT_VALUE, configKey.defaultValue);
        assertEquals(ConfigDef.Importance.HIGH, configKey.importance);
    }

    @Test
    void specConfigAcceptsValidValues() {
        ConfigDef config = cast.config();

        Map<String, ConfigDef.ConfigKey> configKeys = config.configKeys();

        assertNotNull(configKeys);
        assertTrue(configKeys.containsKey(SPEC_CONFIG));
        ConfigDef.ConfigKey configKey = configKeys.get(SPEC_CONFIG);

        configKey.validator.ensureValid("", Arrays.asList("field:string"));
    }

    @Test
    void specConfigRejectsInvalidValues() {
        ConfigDef config = cast.config();

        Map<String, ConfigDef.ConfigKey> configKeys = config.configKeys();

        assertNotNull(configKeys);
        assertTrue(configKeys.containsKey(SPEC_CONFIG));
        ConfigDef.ConfigKey configKey = configKeys.get(SPEC_CONFIG);

        assertThrows(ConfigException.class, () -> {
            configKey.validator.ensureValid("", Arrays.asList());
        });
        assertThrows(ConfigException.class, () -> {
            configKey.validator.ensureValid("", Arrays.asList("invalid"));
        });
        assertThrows(ConfigException.class, () -> {
            configKey.validator.ensureValid("", Arrays.asList(":"));
        });
    }

    private Schema buildBaseSchema() {
        return SchemaBuilder
                .struct().name("base")
                .optional()
                .field(randomKeyString, SchemaBuilder.bytes().name("baseRandomKey"))
                .field(randomSecondKeyString, SchemaBuilder.bytes().name("baseRandomSecondKey"))
                .build();
    }

    private Schema buildNestedSchema() {
        return SchemaBuilder
                .struct().name("nested")
                .field(randomKeyString, SchemaBuilder.bytes().name("nestedRandomKey"))
                .field(randomSecondKeyString, SchemaBuilder.bytes().name("nestedRandomSecondKey"))
                .field("A", buildBaseSchema())
                .build();
    }

    private Schema buildNestedSchemaWithOptional() {
        return SchemaBuilder
                .struct().name("nestedOptional")
                .optional()
                .field(randomKeyString, SchemaBuilder.bytes().name("optionalNestedRandomKey"))
                .field(randomSecondKeyString, SchemaBuilder.bytes().name("optionalNestedRandomKey"))
                .optional()
                .field("A", buildBaseSchema())
                .optional()
                .build();
    }

    private Struct buildBaseStruct(Schema schema) {
        Struct struct = new Struct(schema);
        struct.put(randomKeyString, randomValueString.getBytes());
        struct.put(randomSecondKeyString, randomValueString.getBytes());
        return struct;
    }

    private Struct buildNestedStruct(Schema schema) {
        Struct struct = new Struct(schema);
        struct.put(randomKeyString, randomValueString.getBytes());
        struct.put(randomSecondKeyString, randomValueString.getBytes());
        struct.put("A", buildBaseStruct(schema.field("A").schema()));
        return struct;
    }

    private Struct buildNestedStructWithNull(Schema schema) {
        Struct struct = new Struct(schema);
        struct.put(randomKeyString, randomValueString.getBytes());
        struct.put(randomSecondKeyString, randomValueString.getBytes());
        struct.put("A", null);
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
