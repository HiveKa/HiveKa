# Hive Storage Handler for Kafka

Apache Hive's storage handler to add support in Apache Hive to be able to query data from Apache Kafka

Visit our website: http://hiveka.weebly.com/

To create a Kafka table in Hive run:
```
create external table test_kafka (a int, b string) stored by 'org.apache.hadoop.hive.kafka.KafkaStorageHandler' tblproperties('kafka.topic'='test', 'kafka.service.uri'='hivekafka-1.ent.cloudera.com:9092', 'kafka.whitelist.topics'='test4', 'kafka.avro.schema.file'='/tmp/test.avsc');
```

To generate Avro byte data into a topic, run our DemoProducer and pass the topic, number of messages and a kafka broker as parameters.
For example:
```
java -classpath "/opt/cloudera/parcels/CDH/lib/avro/*:hive-kafka-1.0-SNAPSHOT.jar:/usr/lib/hive/*:/usr/lib/hive/*" org.apache.hadoop.hive.kafka.demoproducer.DemoProducer test4 10 hivekafka-1:9092
```
