package com.kafka.connect.smt;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class UpdateMessage<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

  public static final String OVERVIEW_DOC =
    "Transform a connect record using new schema";

  private interface ConfigName {
    String SCHEMA_FILE = "schema.file";
    String SCHEMA_PLUGIN_LOCATION = "schema.plugin.location";
    String SCHEMA_NAME = "schema.name";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.SCHEMA_FILE, ConfigDef.Type.STRING, "schemafile", ConfigDef.Importance.HIGH,
      "Field name for schema file")
    .define(ConfigName.SCHEMA_PLUGIN_LOCATION, ConfigDef.Type.STRING, "schemapluginlocation", ConfigDef.Importance.HIGH,
            "Plugin location for schema")
    .define(ConfigName.SCHEMA_NAME, ConfigDef.Type.STRING, "schemaname", ConfigDef.Importance.HIGH,
            "Field name for schema name");

  private static final String PURPOSE = "transforming a record using new schema";

  private String schemaFile;
  private String schemaPluginLocation;
  private String schemaName;
  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    schemaFile = config.getString(ConfigName.SCHEMA_FILE);
    schemaPluginLocation = config.getString(ConfigName.SCHEMA_PLUGIN_LOCATION);
    schemaName = config.getString(ConfigName.SCHEMA_NAME);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }


  @Override
  public R apply(R record) {
      return applyWithSchema(record);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

    // Get AVRO schema
    org.apache.avro.Schema avroSchema = ConfigUtils.getSchemaFromSchemaFileName(schemaPluginLocation, schemaFile);
    log.info("AVRO SCHEMA : {}", avroSchema);
    log.info("CONFIG PLUGIN SCHEMA NAME : {}", schemaName);

    // Build AVRO Object
    Object avroDataObj = wrapData(avroSchema, schemaName, value);

    log.info("AVRO VALUE : {}", avroDataObj);

    // Get updated connect schema
    AvroData avroData = new AvroData(1);
    org.apache.kafka.connect.data.Schema updatedSchema = avroData.toConnectSchema(avroSchema);

    // Get updated connect object
    Object updatedValue = avroData.toConnectData(avroSchema, avroDataObj).value();

    log.info("OUT SCHEMA : {}", updatedSchema);
    log.info("OUT VALUE : {}", updatedValue);

    return newRecord(record, updatedSchema, updatedValue);
  }

  private Object wrapData(org.apache.avro.Schema schema, String schemaName, Object data) {
    if (schema.getType() == org.apache.avro.Schema.Type.RECORD && data instanceof Struct) {
      log.info("wrapData PLUGIN SCHEMA TYPE: {}", schema.getType());
      log.info("wrapData PLUGIN DATA : {}, TYPE: {} ", data, data.getClass());

      Struct optionMap = (Struct) data;
      GenericData.Record record = new GenericData.Record(schema);

      for (org.apache.avro.Schema.Field field : schema.getFields()) {
        org.apache.avro.Schema fieldSchema = field.schema();

        for (org.apache.avro.Schema pluginSchema : fieldSchema.getTypes()) {
          log.info("PLUGIN SCHEMA NAME : {}", pluginSchema.getName());
          log.info("CONFIG SCHEMA NAME : {}", schemaName);
          if (pluginSchema.getName().equals(schemaName)) {
            log.info("PLUGIN SCHEMA TYPE : {}", pluginSchema.getType());
            if (pluginSchema.getType() == org.apache.avro.Schema.Type.RECORD) {
              GenericData.Record pluginRecord = new GenericData.Record(pluginSchema);

              AvroData inAvroData = new AvroData(1);
              final GenericData.Record inAvroRecord = (GenericData.Record)inAvroData.fromConnectData(optionMap.schema(), optionMap);
              log.info("IN SCHEMA : {}", optionMap.schema());
              log.info("IN VALUE : {}", inAvroRecord);

              for (org.apache.avro.Schema.Field subField : pluginSchema.getFields()) {
                log.info("PLUGIN DATA : {}", subField.name());
                pluginRecord.put(subField.name(), inAvroRecord.get(subField.name()));
              }

              record.put(field.name(), pluginRecord);
            }

            GenericRecordBuilder optionBuilder = new GenericRecordBuilder(record);

            return optionBuilder.build();
          }
        }
      }
    }
    return data;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends UpdateMessage<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends UpdateMessage<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


