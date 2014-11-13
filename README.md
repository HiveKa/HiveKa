# Hive Storage Handler for Kafka

HiveKa is Apache Hive's storage handler that adds support in Apache Hive to query data from Apache Kafka. This provides an opportunity to Kafka users to inspect data ingested by Kafka without writing complex Kafka consumers. Hive makes it possible to run complex analytical queries across various data sources, like, HDFS, Solr, Hbase, etc.. HiveKa extends this support to Kafka.

Visit our [website](http://hiveka.weebly.com/).

To create a Kafka table in Hive run:
```
create external table test_kafka (a int, b string) stored by 'org.apache.hadoop.hive.kafka.KafkaStorageHandler' tblproperties('kafka.service.uri'='hivekafka-1.ent.cloudera.com:9092', 'kafka.whitelist.topics'='test4', 'kafka.avro.schema.file'='/tmp/test.avsc');
```

To generate Avro byte data into a topic, run our DemoProducer and pass the topic, number of messages and a kafka broker as parameters.
For example:
```
java -classpath "/opt/cloudera/parcels/CDH/lib/avro/*:hive-kafka-1.0-SNAPSHOT.jar:/usr/lib/hive/*:/usr/lib/hive/*" org.apache.hadoop.hive.kafka.demoproducer.DemoProducer test4 10 hivekafka-1:9092
```
