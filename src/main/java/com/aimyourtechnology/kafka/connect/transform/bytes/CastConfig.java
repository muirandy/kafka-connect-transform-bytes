package com.aimyourtechnology.kafka.connect.transform.bytes;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;

import static com.aimyourtechnology.kafka.connect.transform.bytes.Cast.SPEC_CONFIG;

class CastConfig {

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SPEC_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public void ensureValid(String name, Object valueObject) {
                            List<String> value = (List<String>) valueObject;
                            if (value == null || value.isEmpty()) {
                                throw new ConfigException("Must specify at least one field to cast.");
                            }
                            parseFieldTypes(value);
                        }

                        @Override
                        public String toString() {
                            return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
                        }
                    },
                    ConfigDef.Importance.HIGH,
                    "List of fields and the type to cast them to of the form field1:type,field2:type to cast fields of "
                            + "Maps or Structs. A single type to cast the entire value. Valid types are int8, int16, int32, "
                            + "int64, float32, float64, boolean, and string.");

    private static void parseFieldTypes(List<String> value) {
        value.stream().forEach(f -> checkValidity(f));
    }

    private static void checkValidity(String f) {
        if (f.trim().contains(":") && f.trim().indexOf(":") > 0)
            return;
        throw new ConfigException("Must specify at least one field to cast.");
    }
}