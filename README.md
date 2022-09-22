Kafka Connect SMT to transform kafka messages to protobuf schema

This SMT supports transforming Kafka messages
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`updatemessage.type`| Message to be trasformed | String | `` | High |

Example on how to add to your connector:
```
    "transforms": "updatemessage",
    "transforms.updatemessage.type": "com.kafka.connect.smt.UpdateMessage$Value",
    "transforms.updatemessage.schema.file": "<SchemaFileName.avro>",
    "transforms.updatemessage.schema.plugin.location": "<Abosulte path to sub schemas>",
    "transforms.updatemessage.schema.name": "<SubSchemaName>",
```


ToDO
* ~~add support for records without schemas~~

Lots borrowed from the Apache Kafka® `InsertField` SMT
