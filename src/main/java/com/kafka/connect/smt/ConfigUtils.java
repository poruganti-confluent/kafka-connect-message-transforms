package com.kafka.connect.smt;

import java.io.*;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaParseException;
import org.apache.avro.reflect.AvroSchema;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtils {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    public static Schema getSchemaFromSchemaFileName(String schemaPluginLocation, String schemaFileName) {
        Schema.Parser schemaParser = new Parser();
        Schema schema = null;

        try {
            File folder = new File(schemaPluginLocation);
            File[] listOfFiles = folder.listFiles();

            // Load plug-in schema files
            for (File schemaFile : listOfFiles) {
                String plugInSchemaFileName = schemaFile.getCanonicalPath();
                try (InputStream stream = new FileInputStream(plugInSchemaFileName)) {
                    schemaParser.parse(stream);
                } catch (FileNotFoundException e) {
                    log.error("Unable to find the schema plugin files {}", plugInSchemaFileName);
                    throw new ConfigException("Unable to find the schema plugin files");
                } catch (SchemaParseException | IOException e) {
                    log.error("Unable to parse the provided schema", e);
                    throw new ConfigException("Unable to parse the provided schema");
                }
            }
        } catch (Exception e) {
            log.error("Unable to find the schema plugin location {}", schemaPluginLocation);
            throw new ConfigException("Unable to find the schema plugin location");
        }

        // Load main schema file
        try (InputStream stream = new FileInputStream(schemaFileName)) {
            schema = schemaParser.parse(stream);
        } catch (FileNotFoundException e) {
            log.error("Unable to find the schema file");
            throw new ConfigException("Unable to find the schema file");
        } catch (SchemaParseException | IOException e) {
            log.error("Unable to parse the provided schema", e);
            throw new ConfigException("Unable to parse the provided schema");
        }
        return schema;
    }
}
