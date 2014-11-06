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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class KafkaStorageHandler implements HiveStorageHandler {
  private static final Logger LOG = Logger.getLogger(KafkaStorageHandler.class.getName());
  private Configuration conf;

  @Override
  public void setConf(Configuration entries) {
    this.conf = entries;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return KafkaInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return KafkaOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return LazyBinarySerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String,
      String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();
    new KafkaBackedTableProperties().initialize(tableProperties, jobProperties, tableDesc);

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String,
      String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();
    new KafkaBackedTableProperties().initialize(tableProperties, jobProperties, tableDesc);

  }

  /**
   * @param tableDesc
   * @param stringStringMap
   * @deprecated
   */
  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String,
      String> stringStringMap) {
    // do nothing;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    // do nothing;
  }
}
