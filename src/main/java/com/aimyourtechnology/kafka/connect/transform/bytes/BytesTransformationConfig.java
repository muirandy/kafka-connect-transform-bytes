package com.aimyourtechnology.kafka.connect.transform.bytes;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class BytesTransformationConfig extends AbstractConfig {
    public BytesTransformationConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
    }

    static ConfigDef config() {
        return new ConfigDef();
    }
}
