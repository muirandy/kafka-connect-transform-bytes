package com.aimyourtechnology.kafka.connect.transform.bytes;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Map;

public class BytesTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(BytesTransformation.class);
    private BytesTransformationConfig config;

    @Override
    public R apply(R record) {
        if (null == record.valueSchema() || Schema.Type.STRUCT != record.valueSchema().type()) {
            log.trace("record.valueSchema() is null or record.valueSchema() is not a struct.");
            return record;
        }

        Struct inputRecord = (Struct) record.value();
        Struct after = inputRecord.getStruct("after");
        Struct serviceId = after.getStruct("SERVICE_ID");
        byte[] bytes = serviceId.getBytes("value");
        BigInteger big = new BigInteger(bytes);

        Schema newSchema = buildNewSchema(inputRecord.schema());
        Struct returnStruct = new Struct(newSchema);
        returnStruct.put("serviceId", big.toString());

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                returnStruct.schema(),
                returnStruct,
                record.timestamp()
        );
    }

    private Schema buildNewSchema(Schema schema) {
        return SchemaBuilder.struct()
                .doc("Custom Transform")
                .field("serviceId", SchemaBuilder.string().doc("Service_ID").build())
                .build();
    }

    @Override
    public ConfigDef config() {
        return BytesTransformationConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new BytesTransformationConfig(map);
    }
}
