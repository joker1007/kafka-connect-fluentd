package org.fluentd.kafka;

import influent.EventEntry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;
import org.msgpack.value.ValueFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessagePackConverterTest {
    @Test
    public void simpleKeyValue() {
        Map<String, String> map = new HashMap<>();
        map.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "true");
        map.put(FluentdSourceConnectorConfig.KAFKA_TAG_AS_PARTITION_KEY, "true");
        FluentdSourceConnectorConfig config = new FluentdSourceConnectorConfig(map);
        EventEntry eventEntry = EventEntry.of(
                Instant.now(),
                ValueFactory.newMap(
                        ValueFactory.newString("message"),
                        ValueFactory.newString("This is a message."),
                        ValueFactory.newString("flag"),
                        ValueFactory.newBoolean(true)));

        MessagePackConverter converter = new MessagePackConverter(config);
        SourceRecord sourceRecord = converter.convert("topic", "tag", 0L, eventEntry);

        assertEquals(Schema.STRING_SCHEMA, sourceRecord.keySchema());
        assertEquals("tag", sourceRecord.key());
        assertEquals("topic", sourceRecord.valueSchema().name());
        Struct struct = (Struct) sourceRecord.value();
        assertEquals("This is a message.", struct.get("message"));
        assertTrue(struct.getBoolean("flag"));
    }

    @Test
    public void simpleKeyValueWithPartitionKeyField() {
        Map<String, String> map = new HashMap<>();
        map.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "true");
        map.put(FluentdSourceConnectorConfig.KAFKA_PARTITION_KEY_FIELD, "flag");
        FluentdSourceConnectorConfig config = new FluentdSourceConnectorConfig(map);
        EventEntry eventEntry = EventEntry.of(
                Instant.now(),
                ValueFactory.newMap(
                        ValueFactory.newString("message"),
                        ValueFactory.newString("This is a message."),
                        ValueFactory.newString("flag"),
                        ValueFactory.newBoolean(true)));

        MessagePackConverter converter = new MessagePackConverter(config);
        SourceRecord sourceRecord = converter.convert("topic", "tag", 0L, eventEntry);

        assertEquals(Schema.BOOLEAN_SCHEMA, sourceRecord.keySchema());
        assertEquals(true, sourceRecord.key());
        assertEquals("topic", sourceRecord.valueSchema().name());
        Struct struct = (Struct) sourceRecord.value();
        assertEquals("This is a message.", struct.get("message"));
        assertTrue(struct.getBoolean("flag"));
    }

    @Test
    public void simpleKeyValueWithTopicField() {
        Map<String, String> map = new HashMap<>();
        map.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "true");
        map.put(FluentdSourceConnectorConfig.KAFKA_TOPIC_FIELD, "topic");
        FluentdSourceConnectorConfig config = new FluentdSourceConnectorConfig(map);
        EventEntry eventEntry = EventEntry.of(
                Instant.now(),
                ValueFactory.newMap(
                        ValueFactory.newString("message"),
                        ValueFactory.newString("This is a message."),
                        ValueFactory.newString("flag"),
                        ValueFactory.newBoolean(true),
                        ValueFactory.newString("topic"),
                        ValueFactory.newString("topicName")));

        MessagePackConverter converter = new MessagePackConverter(config);
        SourceRecord sourceRecord = converter.convert("topic", "tag", 0L, eventEntry);

        assertNull(sourceRecord.keySchema());
        assertNull(sourceRecord.key());
        assertEquals("topicName", sourceRecord.topic());
        Struct struct = (Struct) sourceRecord.value();
        assertEquals("This is a message.", struct.get("message"));
        assertTrue(struct.getBoolean("flag"));
    }

    @Test
    public void nullValue() {
        Map<String, String> map = new HashMap<>();
        map.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "true");
        map.put(FluentdSourceConnectorConfig.KAFKA_TAG_AS_PARTITION_KEY, "true");
        FluentdSourceConnectorConfig config = new FluentdSourceConnectorConfig(map);
        EventEntry eventEntry = EventEntry.of(
                Instant.now(),
                ValueFactory.newMap(
                        ValueFactory.newString("message"),
                        ValueFactory.newNil()));

        MessagePackConverter converter = new MessagePackConverter(config);
        SourceRecord sourceRecord = converter.convert("topic", "tag", 0L, eventEntry);

        assertEquals(Schema.STRING_SCHEMA, sourceRecord.keySchema());
        assertEquals("tag", sourceRecord.key());
        assertEquals("topic", sourceRecord.valueSchema().name());
        Struct struct = (Struct) sourceRecord.value();
        assertNull(struct.get("message"));
    }

    @Test
    public void nestedMap() {
        Map<String, String> map = new HashMap<>();
        map.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "true");
        map.put(FluentdSourceConnectorConfig.KAFKA_TAG_AS_PARTITION_KEY, "true");
        FluentdSourceConnectorConfig config = new FluentdSourceConnectorConfig(map);
        EventEntry eventEntry = EventEntry.of(
                Instant.now(),
                ValueFactory.newMap(
                        ValueFactory.newString("message"),
                        ValueFactory.newString("This is a message."),
                        ValueFactory.newString("nested"),
                        ValueFactory.newMap(
                                ValueFactory.newString("key"),
                                ValueFactory.newInteger(42)
                        )));

        MessagePackConverter converter = new MessagePackConverter(config);
        SourceRecord sourceRecord = converter.convert("topic", "tag", 0L, eventEntry);

        assertEquals(Schema.STRING_SCHEMA, sourceRecord.keySchema());
        assertEquals("tag", sourceRecord.key());
        assertEquals("topic", sourceRecord.valueSchema().name());
        Struct struct = (Struct) sourceRecord.value();
        assertEquals("This is a message.", struct.get("message"));
        Struct nested = struct.getStruct("nested");
        Long expected = 42L;
        assertEquals(expected, nested.getInt64("key"));
    }

    @Test
    public void nestedArray() {
        Map<String, String> map = new HashMap<>();
        map.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "true");
        map.put(FluentdSourceConnectorConfig.KAFKA_TAG_AS_PARTITION_KEY, "true");
        FluentdSourceConnectorConfig config = new FluentdSourceConnectorConfig(map);
        EventEntry eventEntry = EventEntry.of(
                Instant.now(),
                ValueFactory.newMap(
                        ValueFactory.newString("message"),
                        ValueFactory.newString("This is a message."),
                        ValueFactory.newString("nested"),
                        ValueFactory.newArray(
                                ValueFactory.newFloat(3.14f),
                                ValueFactory.newFloat(42.195f)
                        )));

        MessagePackConverter converter = new MessagePackConverter(config);
        SourceRecord sourceRecord = converter.convert("topic", "tag", 0L, eventEntry);

        assertEquals(Schema.STRING_SCHEMA, sourceRecord.keySchema());
        assertEquals("tag", sourceRecord.key());
        assertEquals("topic", sourceRecord.valueSchema().name());
        Struct struct = (Struct) sourceRecord.value();
        assertEquals("This is a message.", struct.get("message"));
        List<Double> list = struct.getArray("nested");
        assertEquals(list.get(0), 3.14, 0.01);
        assertEquals(list.get(1), 42.195, 0.01);
    }

    @Test
    public void schemalessKeyValue() {
        Map<String, String> map = new HashMap<>();
        map.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "false");
        FluentdSourceConnectorConfig config = new FluentdSourceConnectorConfig(map);
        EventEntry eventEntry = EventEntry.of(
                Instant.now(),
                ValueFactory.newMap(
                        ValueFactory.newString("message"),
                        ValueFactory.newString("This is a message.")));

        MessagePackConverter converter = new MessagePackConverter(config);
        SourceRecord sourceRecord = converter.convert("topic", "tag", 0L, eventEntry);

        Assert.assertNull(sourceRecord.keySchema());
        Assert.assertNull(sourceRecord.key());
        Assert.assertNull(sourceRecord.valueSchema());
        Struct value = (Struct) sourceRecord.value();
        Assert.assertEquals("This is a message.", value.getString("message"));
    }
}
