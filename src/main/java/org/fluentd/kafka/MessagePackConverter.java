/**
 * Copyright 2017 ClearCode Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.fluentd.kafka;

import influent.EventEntry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MessagePackConverter {
    static final Logger log = LoggerFactory.getLogger(MessagePackConverter.class);
    private final FluentdSourceConnectorConfig config;

    public MessagePackConverter(final FluentdSourceConnectorConfig config) {
        this.config = config;
    }

    public SourceRecord convert(String topic, String tag, Long timestamp, EventEntry entry) {
        SchemaAndValue schemaAndValue = convert(topic, entry);
        SchemaAndValue keySchemaAndValue = getKey(schemaAndValue);
        if (keySchemaAndValue.value() == null && config.isFluentdTagAsPartitionKey()) {
            keySchemaAndValue = new SchemaAndValue(Schema.STRING_SCHEMA, tag);
        }
        return new SourceRecord(
                null,
                null,
                getTopic(schemaAndValue, topic),
                null,
                config.isFluentdSchemasEnable() ? keySchemaAndValue.schema() : null,
                keySchemaAndValue.value(),
                config.isFluentdSchemasEnable() ? schemaAndValue.schema() : null,
                schemaAndValue.value(),
                timestamp);
    }

    private String getTopic(SchemaAndValue schemaAndValue, String topic) {
        if (schemaAndValue.schema().type() != Schema.Type.STRUCT) {
            return topic;
        }

        Struct record = (Struct)schemaAndValue.value();
        if (config.getFluentdTopicField() != null) {
            try {
                String topicValue = record.getString(config.getFluentdTopicField());
                if (topicValue != null) {
                    return topicValue;
                }
            } catch (DataException ignored) {
            }
        }
        return topic;
    }

    private SchemaAndValue getKey(SchemaAndValue schemaAndValue) {
        if (config.getFluentdPartitionKeyField() == null) {
            return SchemaAndValue.NULL;
        }

        if (schemaAndValue.schema().type() != Schema.Type.STRUCT) {
            return SchemaAndValue.NULL;
        }

        Struct record = (Struct)schemaAndValue.value();
        try {
            Schema keyFieldSchema = record.schema().field(config.getFluentdPartitionKeyField()).schema();
            Object keyValue = record.get(config.getFluentdPartitionKeyField());
            return new SchemaAndValue(keyFieldSchema, keyValue);
        } catch (DataException ignored) {
            return SchemaAndValue.NULL;
        }
    }

    private SchemaAndValue convert(String topic, EventEntry entry) {
        return convert(topic, entry.getRecord());
    }

    private SchemaAndValue convert(String name, Value value) {
        switch (value.getValueType()) {
            case STRING:
                return new SchemaAndValue(Schema.STRING_SCHEMA, value.asStringValue().asString());
            case NIL:
                return new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, null);
            case BOOLEAN:
                return new SchemaAndValue(Schema.BOOLEAN_SCHEMA, value.asBooleanValue().getBoolean());
            case INTEGER:
                return new SchemaAndValue(Schema.INT64_SCHEMA, value.asIntegerValue().toLong());
            case FLOAT:
                // We cannot identify float32 and float64. We must treat float64 as double.
                return new SchemaAndValue(Schema.FLOAT64_SCHEMA, value.asFloatValue().toDouble());
            case BINARY:
                return new SchemaAndValue(Schema.BYTES_SCHEMA, value.asBinaryValue().asByteArray());
            case MAP: {
                if (config.getFluentdSchemasMapField() != null && config.getFluentdSchemasMapField().contains(name)) {
                    Map<Value, Value> map = value.asMapValue().map();
                    Map<String, SchemaAndValue> fields = new TreeMap<>();
                    map.forEach((k, v) -> {
                        String n = k.asStringValue().asString();
                        fields.put(n, convert(n, v));
                    });
                    SchemaAndValue schemaAndValue = fields.values().iterator().next();
                    Schema valueSchema = schemaAndValue.schema();
                    Map<String, Object> newMap = new TreeMap<>();
                    fields.forEach((k, v) -> {
                        newMap.put(k, v.value());
                    });
                    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema).name(name).build();
                    return new SchemaAndValue(schema, newMap);
                } else {
                    SchemaBuilder builder = SchemaBuilder.struct().name(name);
                    Map<Value, Value> map = value.asMapValue().map();
                    Map<String, SchemaAndValue> fields = new TreeMap<>();
                    map.forEach((k, v) -> {
                        String n = k.asStringValue().asString();
                        fields.put(n, convert(n, v));
                    });
                    fields.forEach((k, v) -> {
                        builder.field(k, v.schema());
                    });
                    Schema schema = builder.build();
                    Struct struct = new Struct(schema);
                    fields.forEach((k, v) -> {
                        struct.put(k, v.value());
                    });
                    return new SchemaAndValue(schema, struct);
                }
            }
            case ARRAY: {
                List<Value> array = value.asArrayValue().list();
                SchemaAndValue sv = convert(name, array.get(0));
                ArrayList<Object> values = new ArrayList<>();
                SchemaBuilder.type(sv.schema().type());
                array.forEach(val -> values.add(convert(null, val).value()));
                Schema schema = SchemaBuilder.array(sv.schema()).optional().build();
                return new SchemaAndValue(schema, values);
            }
            default:
                return null;
        }
    }
}
