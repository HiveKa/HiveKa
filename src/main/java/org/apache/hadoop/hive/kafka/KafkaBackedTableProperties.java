/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaBackedTableProperties {
  private static final Logger LOG = Logger.getLogger(KafkaBackedTableProperties.class);
  public static final String KAFKA_URI = "kafka.service.uri";
  public static final String KAFKA_URL = "kafka.service.url";
  public static final String KAFKA_PORT = "kafka.service.port";
  public static final String KAFKA_WHITELIST_TOPICS = "kafka.whitelist.topics";
  public static final String KAFKA_AVRO_SCHEMA_FILE = "kafka.avro.schema.file";
  public static final String COLUMN_NAMES = "columns";
  public static final String COLUMN_TYPES = "columns.types";
  public static final String COLUMN_COMMENTS = "columns.comments";

  /*
   * This method initializes properties of the external table and populates
   * the jobProperties map with them so that they can be used throughout the job.
   */
  public void initialize(Properties tableProperties, Map<String,String> jobProperties,
                         TableDesc tableDesc) {

    // Set kafka.whitelist.topics in the jobProperty
    String kafkaWhitelistTopics = tableProperties.getProperty(KAFKA_WHITELIST_TOPICS);
    LOG.debug("Kafka whitelist topics : " + kafkaWhitelistTopics);
    jobProperties.put(KAFKA_WHITELIST_TOPICS, kafkaWhitelistTopics);

    // Set kafka.avro.schema.file in the jobProperty
    String kafkaAvroSchemaFile = tableProperties.getProperty(KAFKA_AVRO_SCHEMA_FILE, null);
    if (kafkaAvroSchemaFile != null) {
      LOG.debug("Kafka avro schema file : " + kafkaAvroSchemaFile);
      jobProperties.put(KAFKA_AVRO_SCHEMA_FILE, kafkaAvroSchemaFile);
    }

    // Set kafka.url and kafka.port in the jobProperty
    String kafkaUri = tableProperties.getProperty(KAFKA_URI);
    LOG.debug("Kafka URI : " + kafkaUri);
    jobProperties.put(KAFKA_URI, kafkaUri);
    final String[] uriSplits = kafkaUri.split(":");
    String kafkaUrl = uriSplits[0];
    String kafkaPort = uriSplits[1];
    LOG.debug("Kafka URL : " + kafkaUrl);
    jobProperties.put(KAFKA_URL, kafkaUrl);
    LOG.debug("Kafka PORT : " + kafkaPort);
    jobProperties.put(KAFKA_PORT, kafkaPort);

    // Set column names in the jobProperty
    String columnNames = tableProperties.getProperty(COLUMN_NAMES);
    LOG.debug("Column Names : " + columnNames);
    jobProperties.put(COLUMN_NAMES, columnNames);

    // Set column types in the jobProperty
    String columnTypes = tableProperties.getProperty(COLUMN_TYPES);
    LOG.debug("Column Types : " + columnTypes);
    jobProperties.put(COLUMN_TYPES, columnTypes);

    // Set column types in the jobProperty
    String columnComments = tableProperties.getProperty(COLUMN_COMMENTS);
    LOG.debug("Column Comments : " + columnComments);
    jobProperties.put(COLUMN_COMMENTS, columnComments);
  }
}
